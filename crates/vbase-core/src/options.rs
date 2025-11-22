use std::collections::HashMap;

use vbase_env::Dir;
use vbase_env::Env;

use crate::Database;
use crate::Error;
use crate::Result;
use crate::engine::Handle;

type OpenEngine = Box<dyn FnOnce(u64, Box<dyn Dir>) -> Result<Box<dyn Handle>>>;

/// A database builder.
pub struct Builder {
    pub options: Options,
    pub engines: HashMap<String, OpenEngine>,
    pub error_if_exists: bool,
    pub error_if_not_exist: bool,
}

impl Builder {
    /// Opens a database at the given path.
    pub fn open(self, path: &str) -> Result<Database> {
        if self.error_if_exists && self.error_if_not_exist {
            return Err(Error::InvalidArgument(
                "cannot set both `error_if_exists` and `error_if_not_exist`".into(),
            ));
        }

        Database::open(path, self)
    }
}

/// Options for a database.
#[derive(Debug)]
pub struct Options {
    pub env: Box<dyn Env>,
    pub journal_file_size: usize,
}

/// Options for write operations.
#[derive(Clone, Default)]
pub struct WriteOptions {
    pub(crate) sync: bool,
}

impl WriteOptions {
    /// Creates default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// If true, the write will be synchronized to the storage.
    ///
    /// Default: false
    pub fn sync(mut self, enable: bool) -> Self {
        self.sync = enable;
        self
    }
}
