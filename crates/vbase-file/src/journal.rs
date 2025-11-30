//! A journal file consists of a sequence of fixed-size blocks, except for the
//! last one.
//!
//! Each block contains one or more fragments. Records that do not fit into a
//! single block are broken into fragments.
//!
//!  Block format:
//!
//! |          Block (32KB)         |
//! |----------+----------+---------+
//! | Fragment | Fragment |   ...   |
//!
//! Fragment format:
//!
//! | Checksum (4B) | Size (2B) | Kind (1B) | Data |

use std::ops::Range;

use vbase_env::SequentialFile as _;
use vbase_env::SequentialFileWriter as _;
use vbase_env::boxed::SequentialFile;
use vbase_env::boxed::SequentialFileWriter;
use vbase_util::codec::BytesEncoder;
use vbase_util::codec::Decode;
use vbase_util::codec::Decoder;
use vbase_util::codec::Encode;
use vbase_util::codec::Encoder;
use vbase_util::codec::Varint;
use vbase_util::crc32::checksum_combined;

use crate::Result;
use crate::error::Corrupted;

const BLOCK_SIZE: usize = 32 * 1024;
const BUFFER_SIZE: usize = 32 * BLOCK_SIZE;
const HEADER_SIZE: usize = 7;

/// A sequential journal file reader.
pub struct File {
    file: SequentialFile,
    /// A buffer for reading data from the file.
    buffer: Box<[u8]>,
    /// The current offset in the buffer.
    offset: usize,
    /// The current length of the buffer.
    length: usize,
    /// A buffer for assembling a record.
    record: Vec<u8>,
}

impl File {
    pub fn new(file: SequentialFile) -> Self {
        Self {
            file,
            buffer: vec![0; BUFFER_SIZE].into_boxed_slice(),
            offset: 0,
            length: 0,
            record: Vec::new(),
        }
    }

    /// Returns the path of the file.
    pub fn path(&self) -> &str {
        self.file.path()
    }

    /// Reads a record from the file.
    ///
    /// Returns `Ok(None)` if reaching the end of the file.
    pub fn read(&mut self) -> Result<Option<&[u8]>> {
        self.record.clear();
        let mut is_first = true;
        while let Some(kind) = self.read_fragment()? {
            match kind {
                FragmentKind::Full if is_first => return Ok(Some(&self.record)),
                FragmentKind::First if is_first => is_first = false,
                FragmentKind::Middle if !is_first => {}
                FragmentKind::Last if !is_first => return Ok(Some(&self.record)),
                _ => {
                    return self
                        .path()
                        .corrupted(format!("unexpected fragment kind {kind:?}"));
                }
            }
        }
        Ok(None)
    }
}

impl File {
    fn read_fragment(&mut self) -> Result<Option<FragmentKind>> {
        let remain = BLOCK_SIZE - (self.offset % BLOCK_SIZE);
        if remain < HEADER_SIZE {
            // Skip the padding bytes in the block.
            self.offset += remain;
        }
        if self.offset >= self.length {
            let n = self.file.read_until_end(&mut self.buffer)?;
            if n == 0 {
                return Ok(None);
            }
            self.length = n;
            self.offset = 0;
        }
        if self.length - self.offset < HEADER_SIZE {
            return self.path().corrupted("incomplete fragment");
        }

        let mut dec = &self.buffer[self.offset..self.length];
        let crc = dec.decode::<u32>();
        let size = dec.decode::<u16>() as usize;
        let kind = dec.decode::<FragmentKind>();
        if dec.len() < size {
            return self.path().corrupted(format!(
                "fragment size mismatch (expected {}, got {})",
                size,
                dec.len()
            ));
        }
        let data = dec.remove(size);
        let checksum = kind.checksum_with(data);
        if checksum != crc {
            return self.path().corrupted(format!(
                "fragment checksum mismatch (expected {crc:#x}, got {checksum:#x})"
            ));
        }

        self.record.extend_from_slice(data);
        self.offset += HEADER_SIZE + size;
        Ok(Some(kind))
    }
}

/// A sequential journal file writer.
pub struct FileWriter {
    file: SequentialFileWriter,
    /// A buffer for building fragments.
    buffer: Box<[u8]>,
    /// The current offset in the buffer.
    offset: usize,
    /// The range of the current fragment in the buffer.
    fragment: Range<usize>,
    /// Whether the current fragment is the first fragment of a record.
    is_first_fragment: bool,
}

impl FileWriter {
    pub fn new(file: SequentialFileWriter) -> Self {
        Self {
            file,
            buffer: vec![0; BUFFER_SIZE].into_boxed_slice(),
            offset: 0,
            fragment: 0..0,
            is_first_fragment: true,
        }
    }

    /// Returns the path of the file.
    pub fn path(&self) -> &str {
        self.file.path()
    }

    /// Returns the size of the file.
    pub fn size(&self) -> u64 {
        self.file.offset() + (self.fragment.end - self.offset) as u64
    }

    /// Synchronizes all data to the file.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync().map_err(Into::into)
    }

    /// Writes a record to the file.
    pub fn write<T: AsRef<[u8]>>(&mut self, record: T) -> Result<()> {
        let mut w = self.record();
        w.append(record.as_ref())?;
        w.finish()
    }

    /// Returns a record writer for multi-part records.
    pub fn record(&mut self) -> RecordWriter<'_> {
        RecordWriter { file: self }
    }
}

impl FileWriter {
    fn flush(&mut self) -> Result<()> {
        assert!(
            self.fragment.is_empty(),
            "cannot flush with started fragment"
        );
        let range = self.offset..self.fragment.start;
        if !range.is_empty() {
            self.file.write_exact(&self.buffer[range])?;
            // Adjust the offset for the last block.
            self.offset = (self.file.offset() % BLOCK_SIZE as u64) as usize;
            self.fragment = self.offset..self.offset;
        }
        Ok(())
    }

    /// Appends data to the current record.
    fn append(&mut self, mut data: &[u8]) -> Result<()> {
        loop {
            if self.fragment.is_empty() {
                self.start_fragment()?;
            }
            let end = self.fragment.end;
            let len = BLOCK_SIZE - (end % BLOCK_SIZE);
            let len = data.len().min(len);
            let buf = data.split_off(..len).unwrap();
            self.buffer[end..end + buf.len()].copy_from_slice(buf);
            self.fragment.end += buf.len();
            if data.is_empty() {
                break;
            }
            self.build_fragment(false);
        }
        Ok(())
    }

    /// Starts a new fragment.
    fn start_fragment(&mut self) -> Result<()> {
        let offset = self.fragment.start;
        let remain = BLOCK_SIZE - (offset % BLOCK_SIZE);
        if remain < HEADER_SIZE {
            // Pad the remaining space in the block with zeros.
            self.buffer[offset..offset + remain].fill(0);
            self.fragment.start += remain;
        }
        if self.fragment.start == self.buffer.len() {
            self.flush()?;
        }
        self.fragment.end = self.fragment.start + HEADER_SIZE;
        Ok(())
    }

    /// Builds the current fragment.
    fn build_fragment(&mut self, is_last: bool) {
        let kind = match (self.is_first_fragment, is_last) {
            (true, true) => FragmentKind::Full,
            (true, false) => FragmentKind::First,
            (false, true) => FragmentKind::Last,
            (false, false) => FragmentKind::Middle,
        };
        let (mut enc, data) = self.buffer[self.fragment.clone()].split_at_mut(HEADER_SIZE);
        enc.encode(kind.checksum_with(data));
        enc.encode(data.len() as u16);
        enc.encode(kind);
        self.fragment.start = self.fragment.end;
        self.is_first_fragment = is_last;
    }
}

/// A journal record writer for multi-part records.
pub struct RecordWriter<'a> {
    file: &'a mut FileWriter,
}

impl<'a> RecordWriter<'a> {
    /// Appends a slice to the record.
    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        self.file.append(data)
    }

    /// Appends a varint to the record.
    pub fn append_varint<T: Varint>(&mut self, value: T) -> Result<()> {
        let mut buf = [0; 16];
        let mut enc = BytesEncoder::new(&mut buf);
        enc.encode_varint(value);
        self.file.append(enc.encoded_bytes())
    }

    /// Appends a varint-prefixed slice to the record.
    pub fn append_varint_slice(&mut self, data: &[u8]) -> Result<()> {
        self.append_varint(data.len())?;
        self.append(data)
    }

    /// Finishes the record.
    pub fn finish(self) -> Result<()> {
        self.file.build_fragment(true);
        self.file.flush()
    }
}

/// DO NOT change the values in this enum.
#[repr(u8)]
#[derive(Copy, Clone, Debug)]
enum FragmentKind {
    // This fragment is a full record
    Full = 1,
    // This fragment is the first part of a new record
    First = 2,
    // This fragment is in the middle of the current record
    Middle = 3,
    // This fragment is the last part of the current record
    Last = 4,
}

impl FragmentKind {
    fn checksum_with(self, data: &[u8]) -> u32 {
        checksum_combined(&[self as u8], data)
    }
}

impl From<u8> for FragmentKind {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Full,
            2 => Self::First,
            3 => Self::Middle,
            4 => Self::Last,
            x => panic!("invalid fragment kind {x}"),
        }
    }
}

impl Encode for FragmentKind {
    fn size(&self) -> usize {
        1
    }

    fn encode_to<E: Encoder>(self, enc: &mut E) {
        enc.put(self as u8);
    }
}

impl<'de> Decode<'de> for FragmentKind {
    fn decode_from<D: Decoder<'de>>(dec: &mut D) -> Self {
        dec.pop().into()
    }
}

#[cfg(test)]
mod tests {
    use vbase_env::boxed::Dir;

    use super::*;

    #[test]
    fn test() -> Result<()> {
        let dir = Dir::test()?;
        let name = "test";
        let records = [
            // Force the next write to pad the block
            vec![1; BLOCK_SIZE - HEADER_SIZE - 1],
            // Two fragments
            vec![2; BLOCK_SIZE],
            // Three fragments
            vec![3; BLOCK_SIZE * 2],
            // Force this write to flush the buffer
            vec![4; BUFFER_SIZE],
        ];
        {
            let mut file = dir.create_sequential_file(name).map(FileWriter::new)?;
            for record in &records {
                file.write(record)?;
            }
            let mut record = file.record();
            record.append_varint_slice(b"foobar")?;
            record.finish()?;
        }
        {
            let mut file = dir.open_sequential_file(name).map(File::new)?;
            for record in &records {
                assert_eq!(file.read()?, Some(record.as_slice()));
            }
            let mut record = Vec::new();
            record.encode(b"foobar".as_slice());
            assert_eq!(file.read()?, Some(record.as_slice()));
            assert_eq!(file.read()?, None);
        }
        Ok(())
    }
}
