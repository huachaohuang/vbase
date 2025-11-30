use std::collections::HashMap;

use vbase_env::boxed::Dir;
use vbase_env::boxed::Env;

use crate::Error;
use crate::Result;
use crate::engine::Engine;
use crate::engine::internal::EngineHandle;

type OpenEngine = Box<dyn FnOnce(u64, Dir) -> Result<Box<dyn EngineHandle>>>;

/// A database builder.
///
/// Public all fields for upper-level wrappers.
#[derive(Default)]
pub struct Builder {
    pub engines: HashMap<String, OpenEngine>,
    pub error_if_exists: bool,
    pub error_if_not_exist: bool,
}

impl Builder {
    /// Creates a default builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an engine.
    pub fn engine<E: Engine>(mut self) -> Self {
        let open = |id, dir| E::open(id, dir).map(|h| Box::new(h) as _);
        self.engines.insert(E::NAME.into(), Box::new(open));
        self
    }

    /// Validates the builder.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.error_if_exists && self.error_if_not_exist {
            return Err(Error::InvalidArgument(
                "cannot set both `error_if_exists` and `error_if_not_exist`".into(),
            ));
        }
        Ok(())
    }
}

/// Options for a database.
#[derive(Clone, Debug)]
pub struct Options {
    pub(crate) env: Env,
    pub(crate) journal_file_size: usize,
}

impl Options {
    /// Creates default options.
    pub fn new() -> Self {
        Self::with_env(Env::default())
    }

    /// Creates options for tests.
    #[cfg(feature = "test")]
    pub fn test() -> Result<Self> {
        let env = Env::test()?;
        Ok(Self::with_env(env))
    }

    /// Creates options with the given environment.
    fn with_env(env: Env) -> Self {
        Self {
            env,
            journal_file_size: 64 << 20,
        }
    }

    /// Validates the options.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.journal_file_size == 0 {
            return Err(Error::InvalidArgument(
                "`journal_file_size` must not be 0".into(),
            ));
        }
        Ok(())
    }
}

impl Default for Options {
    fn default() -> Self {
        Self::new()
    }
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
