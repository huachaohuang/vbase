use vbase_env::boxed::SequentialFile;
use vbase_env::boxed::SequentialFileWriter;
use vbase_file::journal::File;
use vbase_file::journal::FileWriter;
use vbase_util::codec::BytesEncoder;
use vbase_util::codec::Decoder;
use vbase_util::codec::Encoder;

use crate::Result;

/// A journal file reader.
pub(crate) struct Journal(File);

impl Journal {
    pub(crate) fn new(file: SequentialFile) -> Self {
        Self(File::new(file))
    }

    /// Reads a batch from the file.
    pub(crate) fn read(&mut self) -> Result<Option<(u64, &[u8])>> {
        match self.0.read()? {
            Some(mut record) => {
                let lsn = record.decode_varint();
                Ok(Some((lsn, record)))
            }
            None => Ok(None),
        }
    }
}

/// A journal file writer.
pub(crate) struct JournalWriter(FileWriter);

impl JournalWriter {
    pub(crate) fn new(file: SequentialFileWriter) -> Self {
        Self(FileWriter::new(file))
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        self.0.sync().map_err(Into::into)
    }

    /// Writes a batch to the file.
    pub(crate) fn write(&mut self, lsn: u64, batch: &[u8]) -> Result<()> {
        let mut buf = [0; 32];
        let mut enc = BytesEncoder::new(&mut buf);
        enc.encode_varint(lsn);
        self.0.write_vectored(&[enc.encoded_bytes(), batch])?;
        Ok(())
    }
}
