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

use vbase_env::SequentialFile as _;
use vbase_env::SequentialFileWriter as _;
use vbase_env::boxed::SequentialFile;
use vbase_env::boxed::SequentialFileWriter;
use vbase_util::codec::Decoder;
use vbase_util::codec::Encoder;
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
        let kind = FragmentKind::from(dec.decode::<u8>());
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
    /// A buffer for caching data before flushing to the file.
    buffer: Box<[u8]>,
    /// The current offset in the buffer.
    offset: usize,
    /// The current length of the buffer.
    length: usize,
}

impl FileWriter {
    pub fn new(file: SequentialFileWriter) -> Self {
        Self {
            file,
            buffer: vec![0; BUFFER_SIZE].into_boxed_slice(),
            offset: 0,
            length: 0,
        }
    }

    /// Returns the path of the file.
    pub fn path(&self) -> &str {
        self.file.path()
    }

    /// Returns the size of the file.
    pub fn size(&self) -> u64 {
        self.file.offset() + (self.length - self.offset) as u64
    }

    /// Synchronizes all data to the file.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync().map_err(Into::into)
    }

    /// Writes a record to the file.
    pub fn write<T: AsRef<[u8]>>(&mut self, record: T) -> Result<()> {
        self.write_part(record.as_ref(), true, true)?;
        self.flush()
    }

    /// Writes a multi-part record to the file.
    ///
    /// This is useful to avoid extra copies when the record is not contiguous.
    pub fn write_vectored<T: AsRef<[u8]>>(&mut self, parts: &[T]) -> Result<()> {
        for (i, part) in parts.iter().enumerate() {
            self.write_part(part.as_ref(), i == 0, i == parts.len() - 1)?;
        }
        self.flush()
    }
}

impl FileWriter {
    fn flush(&mut self) -> Result<()> {
        if self.offset < self.length {
            self.file
                .write_exact(&self.buffer[self.offset..self.length])?;
            // Adjust the offset for the last block.
            self.offset = (self.file.offset() % BLOCK_SIZE as u64) as usize;
            self.length = self.offset;
        }
        Ok(())
    }

    fn write_part(&mut self, part: &[u8], mut is_first: bool, is_last: bool) -> Result<()> {
        let mut left = part;
        while !left.is_empty() {
            let size = self.spare_fragment_size()?;
            let size = left.len().min(size);
            let data = left.split_off(..size).unwrap();
            let kind = match (is_first, is_last && left.is_empty()) {
                (true, true) => FragmentKind::Full,
                (true, false) => FragmentKind::First,
                (false, true) => FragmentKind::Last,
                (false, false) => FragmentKind::Middle,
            };
            is_first = false;
            self.write_fragment(kind, data)?;
        }
        Ok(())
    }

    fn write_fragment(&mut self, kind: FragmentKind, data: &[u8]) -> Result<()> {
        let mut enc = &mut self.buffer[self.length..];
        enc.encode(kind.checksum_with(data));
        enc.encode(data.len() as u16);
        enc.encode(kind as u8);
        enc.append(data);
        self.length += HEADER_SIZE + data.len();
        if self.length == self.buffer.len() {
            self.flush()?;
        }
        Ok(())
    }

    fn spare_fragment_size(&mut self) -> Result<usize> {
        let mut remain = BLOCK_SIZE - (self.length % BLOCK_SIZE);
        if remain < HEADER_SIZE {
            // Pad the remaining space in the block with zeros.
            self.buffer[self.length..self.length + remain].fill(0);
            self.length += remain;
            remain = BLOCK_SIZE;
        }
        if self.length == self.buffer.len() {
            self.flush()?;
            remain = BLOCK_SIZE;
        }
        Ok(remain - HEADER_SIZE)
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
            file.write_vectored(&["foo"])?;
            file.write_vectored(&["foo", "bar"])?;
            file.write_vectored(&["foo", "bar", "baz"])?;
        }
        {
            let mut file = dir.open_sequential_file(name).map(File::new)?;
            for record in &records {
                assert_eq!(file.read()?, Some(record.as_slice()));
            }
            assert_eq!(file.read()?, Some("foo".as_bytes()));
            assert_eq!(file.read()?, Some("foobar".as_bytes()));
            assert_eq!(file.read()?, Some("foobarbaz".as_bytes()));
            assert_eq!(file.read()?, None);
        }
        Ok(())
    }
}
