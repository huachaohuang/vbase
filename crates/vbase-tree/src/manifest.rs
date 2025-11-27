use prost::Message;
use vbase_engine::env::boxed::SequentialFile;
use vbase_engine::env::boxed::SequentialFileWriter;
use vbase_engine::file::journal::File;
use vbase_engine::file::journal::FileWriter;

use crate::Result;
use crate::error::Corrupted;

#[derive(Message)]
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct Desc {
    #[prost(tag = "1", uint64)]
    pub(crate) last_id: u64,
    #[prost(tag = "2", repeated, message)]
    pub(crate) buckets: Vec<BucketDesc>,
}

impl Desc {
    fn merge(&mut self, mut edit: Edit) {
        self.last_id = self.last_id.max(edit.last_id);
        self.buckets.append(&mut edit.add_buckets);
        for id in edit.delete_buckets {
            self.buckets.retain(|desc| desc.id != id);
        }
    }
}

#[derive(Message)]
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct Edit {
    #[prost(tag = "1", uint64)]
    pub(crate) last_id: u64,
    #[prost(tag = "2", repeated, message)]
    pub(crate) add_buckets: Vec<BucketDesc>,
    #[prost(tag = "3", repeated, uint64)]
    pub(crate) delete_buckets: Vec<u64>,
}

#[derive(Message)]
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct BucketDesc {
    #[prost(tag = "1", uint64)]
    pub(crate) id: u64,
    #[prost(tag = "2", string)]
    pub(crate) name: String,
}

/// A manifest file reader.
pub(crate) struct Manifest {
    file: File,
}

impl Manifest {
    fn new(file: SequentialFile) -> Self {
        Self {
            file: File::new(file),
        }
    }

    /// Loads a [`Desc`] from the file.
    pub(crate) fn load(file: SequentialFile) -> Result<Desc> {
        let mut this = Self::new(file);
        let mut desc = Desc::default();
        while let Some(edit) = this.read()? {
            desc.merge(edit);
        }
        Ok(desc)
    }

    /// Reads an [`Edit`] from the file.
    pub(crate) fn read(&mut self) -> Result<Option<Edit>> {
        match self.file.read()? {
            Some(record) => Edit::decode(record)
                .map(Some)
                .or_else(|e| self.file.path().corrupted(format!("{e}"))),
            None => Ok(None),
        }
    }
}

/// A manifest file writer.
pub(crate) struct ManifestWriter {
    desc: Desc,
    file: FileWriter,
    /// The initial size of the current file.
    ///
    /// This is used to determine when to switch to a new file.
    init_size: u64,
}

impl ManifestWriter {
    const MIN_FILE_SIZE: u64 = 1024 * 1024;

    pub(crate) fn open(desc: Desc, file: SequentialFileWriter) -> Result<Self> {
        let mut this = Self {
            desc,
            file: FileWriter::new(file),
            init_size: 0,
        };
        this.init_file()?;
        Ok(this)
    }

    /// Writes an edit to the file.
    pub(crate) fn write(&mut self, edit: Edit) -> Result<()> {
        self.file.write(edit.encode_to_vec())?;
        self.file.sync()?;
        self.desc.merge(edit);
        Ok(())
    }

    fn init_file(&mut self) -> Result<()> {
        self.file.write(self.desc.encode_to_vec())?;
        self.file.sync()?;
        self.init_size = self.file.size();
        Ok(())
    }

    /// Switches to the given file.
    pub(crate) fn switch_file(&mut self, id: u64, file: SequentialFileWriter) -> Result<()> {
        self.desc.last_id = id;
        self.file = FileWriter::new(file);
        self.init_file()
    }

    /// Returns true if the current file should be switched.
    pub(crate) fn should_switch_file(&self) -> bool {
        self.file.size() >= (self.init_size * 2).max(Self::MIN_FILE_SIZE)
    }
}
