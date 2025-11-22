use std::collections::HashMap;

use vbase_env::Env;
use vbase_env::LocalEnv;
use vbase_util::sync::Arc;

use crate::Result;
use crate::WriteBatch;
use crate::WriteOptions;
use crate::engine::Engine;

/// A database builder.
pub struct Builder(vbase_core::Builder);

impl Builder {
    /// Creates a default builder.
    pub fn new() -> Self {
        Self::with_env(LocalEnv)
    }

    /// Creates a builder with the given environment.
    fn with_env<E: Env + 'static>(env: E) -> Self {
        let options = vbase_core::Options {
            env: Arc::new(env),
            journal_file_size: 64 << 20,
        };
        Self(vbase_core::Builder {
            options,
            engines: HashMap::new(),
            error_if_exists: false,
            error_if_not_exist: false,
        })
    }

    /// Registers an engine.
    fn with_engine<E: Engine>(mut self) -> Self {
        let open = |id, dir| E::open(id, dir);
        self.0.engines.insert(E::NAME.into(), Box::new(open));
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

    /// Writes a batch to the database.
    pub fn write(&self, batch: &WriteBatch, options: &WriteOptions) -> Result<()> {
        self.0.write(batch, options)
    }

    /// Gets a collection.
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

#[cfg(test)]
mod tests {
    use vbase_env::TestEnv;

    use crate::Builder;
    use crate::Database;
    use crate::Error;
    use crate::Result;
    use crate::engine::test::Engine;

    const PATH: &'static str = "test";

    fn test_builder() -> Builder {
        let env = TestEnv::new().unwrap();
        Builder::with_env(env)
    }

    fn test_database() -> Result<Database> {
        test_builder().with_engine::<Engine>().open(PATH)
    }

    #[test]
    fn test_unregistered_engine() -> Result<()> {
        let db = test_builder().open(PATH)?;
        match db.collection::<Engine>("test") {
            Err(Error::InvalidArgument(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        match db.create_collection::<Engine>("test") {
            Err(Error::InvalidArgument(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        match db.delete_collection::<Engine>("test") {
            Err(Error::InvalidArgument(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_create_delete_collection() -> Result<()> {
        let db = test_database()?;
        match db.collection::<Engine>("test") {
            Err(Error::NotExist(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        db.create_collection::<Engine>("test")?;
        match db.create_collection::<Engine>("test") {
            Err(Error::Exists(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        db.collection::<Engine>("test")?;
        db.delete_collection::<Engine>("test")?;
        match db.delete_collection::<Engine>("test") {
            Err(Error::NotExist(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        match db.collection::<Engine>("test") {
            Err(Error::NotExist(_)) => {}
            x => panic!("unexpected result: {x:?}"),
        }
        Ok(())
    }
}
