use std::collections::HashMap;

use vbase_core::engine::Engine;
use vbase_env::LocalEnv;
use vbase_util::sync::Arc;

use crate::Result;

/// A database builder.
pub struct Builder(vbase_core::Builder);

impl Builder {
    /// Creates a default builder.
    pub fn new() -> Self {
        let options = vbase_core::Options {
            env: Arc::new(LocalEnv),
            journal_file_size: 64 << 20,
        };
        Self(vbase_core::Builder {
            options,
            engines: HashMap::new(),
            error_if_exists: false,
            error_if_not_exist: false,
        })
    }

    /// If true, returns an error if the database already exists.
    ///
    /// Conflicts with [`Self::error_if_not_exist`].
    ///
    /// Default: false
    pub fn error_if_exists(mut self, enable: bool) -> Self {
        self.0.error_if_exists = enable;
        self
    }

    /// If true, returns an error if the database does not exist.
    ///
    /// Conflicts with [`Self::error_if_exists`].
    ///
    /// Default: false
    pub fn error_if_not_exist(mut self, enable: bool) -> Self {
        self.0.error_if_not_exist = enable;
        self
    }

    /// Opens a database at the given path.
    ///
    /// By default, the builder creates the database if it does not exist.
    /// This behavior can be changed by:
    ///
    /// - [`Self::error_if_exists`]
    /// - [`Self::error_if_not_exist`]
    ///
    /// The opened database locks `path` for exclusive access. Attempt to open
    /// the same database again will result in an error.
    pub fn open(self, path: &str) -> Result<Database> {
        let core = self.0.open(path)?;
        Ok(Database(Arc::new(core)))
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

/// A multi-model embedded database.
#[derive(Clone, Debug)]
pub struct Database(Arc<vbase_core::Database>);

impl Database {
    /// Opens a database at the given path with default options.
    ///
    /// This function creates the database if it does not exist.
    ///
    /// This is equivalent to `Builder::new().open(path)`.
    /// See [`Builder::open`] for more details.
    pub fn open(path: &str) -> Result<Self> {
        Builder::new().open(path)
    }

    /// Returns a collection.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotExist`] if `name` does not exist.
    pub fn collection<E: Engine>(&self, name: &str) -> Result<E::Collection> {
        self.0.collection::<E>(name)
    }

    /// Creates a collection.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Exists`] if `name` already exists.
    pub fn create_collection<E: Engine>(&self, name: &str) -> Result<E::Collection> {
        self.0.create_collection::<E>(name)
    }

    /// Deletes a collection.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotExist`] if `name` does not exist.
    pub fn delete_collection<E: Engine>(&self, name: &str) -> Result<()> {
        self.0.delete_collection::<E>(name)
    }
}
