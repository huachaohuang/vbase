use vbase_core::Core;
use vbase_core::options;
use vbase_util::sync::Arc;

#[rustfmt::skip]
#[allow(unused_imports, reason = "for doc comments")]
use crate::Error;

use crate::Engine;
use crate::Options;
use crate::Result;
use crate::WriteBatch;
use crate::WriteOptions;

/// A database builder.
pub struct Builder(options::Builder);

impl Builder {
    /// Creates a default builder.
    pub fn new() -> Self {
        Self(options::Builder::new())
    }

    /// Registers an engine.
    pub fn engine<E: Engine>(mut self) -> Self {
        self.0 = self.0.engine::<E>();
        self
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
    ///
    /// # Errors
    ///
    /// - Returns [`Error::Locked`] if the database is already opened.
    /// - Returns [`Error::Exists`] if `error_if_exists` is true and the
    ///   database already exists.
    /// - Returns [`Error::NotExist`] if `error_if_not_exist` is true and the
    ///   database does not exist.
    pub fn open(self, path: &str, options: Options) -> Result<Database> {
        let core = Core::open(path, options, self.0)?;
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
pub struct Database(Arc<Core>);

impl Database {
    /// Opens a database at the given path.
    ///
    /// This function creates the database if it does not exist.
    ///
    /// This is equivalent to `Builder::new().open(path, options)`.
    /// See [`Builder::open`] for more details.
    pub fn open(path: &str, options: Options) -> Result<Self> {
        Builder::new().open(path, options)
    }

    /// Writes a batch to the database.
    pub fn write(&self, batch: &WriteBatch, options: &WriteOptions) -> Result<()> {
        self.0.write(batch, options)
    }

    /// Gets a bucket from the engine if it exists.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotExist`] if `name` does not exist.
    pub fn bucket<E: Engine>(&self, name: &str) -> Result<E::Bucket> {
        self.0.bucket::<E>(name)
    }

    /// Creates a bucket in the engine.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Exists`] if `name` already exists.
    pub fn create_bucket<E: Engine>(&self, name: &str) -> Result<E::Bucket> {
        self.0.create_bucket::<E>(name)
    }

    /// Deletes a bucket from the engine.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotExist`] if `name` does not exist.
    pub fn delete_bucket<E: Engine>(&self, name: &str) -> Result<()> {
        self.0.delete_bucket::<E>(name)
    }
}

#[cfg(test)]
mod tests {
    use crate::Builder;
    use crate::Database;
    use crate::Error;
    use crate::Options;
    use crate::Result;
    use crate::tree::Engine;

    const PATH: &'static str = "test";

    fn test_database() -> Result<Database> {
        let options = Options::test()?;
        Builder::new().engine::<Engine>().open(PATH, options)
    }

    #[test]
    fn test_builder() -> Result<()> {
        let options = Options::test()?;
        match Builder::new()
            .error_if_exists(true)
            .error_if_not_exist(true)
            .open(PATH, options.clone())
        {
            Err(Error::InvalidArgument(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        match Builder::new()
            .error_if_not_exist(true)
            .open(PATH, options.clone())
        {
            Err(Error::NotExist(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        Builder::new().open(PATH, options.clone())?;
        match Builder::new()
            .error_if_exists(true)
            .open(PATH, options.clone())
        {
            Err(Error::Exists(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_open_locked() -> Result<()> {
        let options = Options::test()?;
        let _locked = Database::open(PATH, options.clone())?;
        match Database::open(PATH, options) {
            Err(Error::Locked(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_unregistered_engine() -> Result<()> {
        let options = Options::test()?;
        let db = Database::open(PATH, options)?;
        match db.bucket::<Engine>("test") {
            Err(Error::InvalidArgument(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        match db.create_bucket::<Engine>("test") {
            Err(Error::InvalidArgument(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        match db.delete_bucket::<Engine>("test") {
            Err(Error::InvalidArgument(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_create_delete_bucket() -> Result<()> {
        let db = test_database()?;
        match db.bucket::<Engine>("test") {
            Err(Error::NotExist(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        db.create_bucket::<Engine>("test")?;
        match db.create_bucket::<Engine>("test") {
            Err(Error::Exists(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        db.bucket::<Engine>("test")?;
        db.delete_bucket::<Engine>("test")?;
        match db.delete_bucket::<Engine>("test") {
            Err(Error::NotExist(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        match db.bucket::<Engine>("test") {
            Err(Error::NotExist(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        Ok(())
    }
}
