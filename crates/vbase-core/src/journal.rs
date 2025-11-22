use vbase_env::SequentialFile;
use vbase_env::SequentialFileWriter;
use vbase_file::journal::File;
use vbase_file::journal::FileWriter;
use vbase_util::codec::BytesEncoder;
use vbase_util::codec::Decoder;
use vbase_util::codec::Encoder;

use crate::Result;

/// A journal file reader.
pub(crate) struct Journal {
    file: File,
}

impl Journal {
    pub(crate) fn open(file: Box<dyn SequentialFile>, name: String) -> Self {
        let file = File::open(file, name);
        Self { file }
    }

    /// Reads a batch from the file.
    pub(crate) fn read(&mut self) -> Result<Option<(u64, &[u8])>> {
        match self.file.read()? {
            Some(mut record) => {
                let lsn = record.decode_varint();
                Ok(Some((lsn, record)))
            }
            None => Ok(None),
        }
    }
}

/// A journal file writer.
pub(crate) struct JournalWriter {
    file: FileWriter,
}

impl JournalWriter {
    pub(crate) fn open(file: Box<dyn SequentialFileWriter>, name: String) -> Self {
        let file = FileWriter::open(file, name);
        Self { file }
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        self.file.sync()?;
        Ok(())
    }

    /// Writes a batch to the file.
    pub(crate) fn write(&mut self, lsn: u64, batch: &[u8]) -> Result<()> {
        let mut buf = [0; 32];
        let mut enc = BytesEncoder::new(&mut buf);
        enc.encode_varint(lsn);
        self.file.write_vectored(&[enc.encoded_bytes(), batch])?;
        Ok(())
    }
}
