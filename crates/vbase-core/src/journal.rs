use vbase_env::boxed::SequentialFile;
use vbase_env::boxed::SequentialFileWriter;
use vbase_file::journal::File;
use vbase_file::journal::FileWriter;
use vbase_file::journal::RecordWriter;
use vbase_util::codec::Decoder;

use crate::Result;

/// A journal file reader.
pub(crate) struct Journal(File);

impl Journal {
    pub(crate) fn new(file: SequentialFile) -> Self {
        Self(File::new(file))
    }

    pub(crate) fn path(&self) -> &str {
        self.0.path()
    }

    /// Reads a batch with its LSN from the file.
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

    /// Writes a batch with its LSN to the file.
    pub(crate) fn write<F>(&mut self, lsn: u64, append: F) -> Result<()>
    where
        F: FnOnce(&mut RecordWriter) -> Result<()>,
    {
        let mut record = self.0.record();
        record.append_varint(lsn)?;
        append(&mut record)?;
        record.finish()?;
        Ok(())
    }
}
