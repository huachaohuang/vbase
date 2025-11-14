use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};

use vbase_env::{Env, LocalEnv};

use crate::collections::private::Handle as CollectionHandle;
use crate::collections::{Collection, Options as CollectionOptions};
use crate::error::{Error, Result};
use crate::root::RootDir;

/// Database options.
pub struct Options {
    env: Box<dyn Env>,
}

impl Options {
    /// Creates a default options.
    pub fn new() -> Self {
        Self {
            env: Box::new(LocalEnv),
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self::new()
    }
}

/// A database builder.
pub struct Builder {
    error_if_exists: bool,
    error_if_not_exist: bool,
}

impl Builder {
    /// Creates a default builder.
    pub fn new() -> Self {
        Self {
            error_if_exists: false,
            error_if_not_exist: false,
        }
    }

    /// If enabled, returns an error if the database already exists.
    ///
    /// Default: false
    pub fn error_if_exists(mut self, enable: bool) -> Self {
        self.error_if_exists = enable;
        self
    }

    /// If enabled, returns an error if the database does not exist.
    ///
    /// Default: false
    pub fn error_if_not_exist(mut self, enable: bool) -> Self {
        self.error_if_not_exist = enable;
        self
    }

    /// Opens a database.
    ///
    /// This function creates the database if it does not exist when both
    /// [`Self::error_if_exists`] and [`Self::error_if_not_exist`] are not
    /// enabled.
    ///
    /// An opened database locks `path` for exclusive access. Attempt to open
    /// the same database again will result in an error.
    pub fn open(self, path: &str, options: Options) -> Result<Database> {
        let handle = DatabaseHandle::open(path, options, self)?;
        Ok(Database(Arc::new(handle)))
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct Database(Arc<DatabaseHandle>);

impl Database {
    /// Opens a database.
    ///
    /// This function creates the database if it does not exist.
    ///
    /// This is equivalent to `Builder::new().open(path, options)`. See
    /// [`Builder::open`] for more details.
    pub fn open(path: &str, options: Options) -> Result<Self> {
        Builder::new().open(path, options)
    }

    /// Creates a collection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::AlreadyExists`] if `name` already exists.
    pub fn create_collection<C>(&self, name: &str, options: C::Options) -> Result<C>
    where
        C: Collection,
    {
        let handle = self.0.create_collection(name, options.into())?;
        C::open(self.clone(), handle)
    }

    /// Deletes a collection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotExist`] if `name` does not exist.
    pub fn delete_collection(&self, name: &str) -> Result<()> {
        self.0.delete_collection(name)
    }
}

struct DatabaseHandle {
    dir: RootDir,
    next_id: AtomicU64,
    collections: Mutex<HashMap<String, CollectionHandle>>,
}

impl DatabaseHandle {
    fn open(path: &str, options: Options, builder: Builder) -> Result<Self> {
        let dir = match options.env.open_dir(path) {
            Ok(dir) => dir,
            Err(e) if e.kind() != ErrorKind::NotFound => {
                return Err(Error::io(e, format!("open database '{path}'")));
            }
            Err(_) if builder.error_if_not_exist => {
                return Err(Error::NotExist(format!("database '{path}'")));
            }
            Err(_) => options
                .env
                .create_dir(path)
                .map_err(|e| Error::io(e, format!("create database '{path}'")))?,
        };

        let dir = RootDir::lock(dir, path.into())?;
        match dir.read_current()? {
            Some(_) if builder.error_if_exists => {
                return Err(Error::AlreadyExists(format!("database '{path}'")));
            }
            Some(id) => todo!(),
            None if builder.error_if_not_exist => {
                return Err(Error::NotExist(format!("database '{path}'")));
            }
            None => (),
        }

        Ok(Self {
            dir,
            next_id: AtomicU64::new(1),
            collections: Mutex::new(HashMap::new()),
        })
    }

    fn create_collection(
        &self,
        name: &str,
        options: CollectionOptions,
    ) -> Result<CollectionHandle> {
        let mut collections = self.collections.lock().unwrap();
        if collections.contains_key(name) {
            return Err(Error::AlreadyExists(format!("collection '{name}'")));
        }

        let id = self.next_id.fetch_add(1, Relaxed);
        let dir = self.dir.create_collection(id)?;
        let handle = CollectionHandle::open(dir, options)?;
        collections.insert(name.into(), handle.clone());
        Ok(handle)
    }

    fn delete_collection(&self, name: &str) -> Result<()> {
        let mut collections = self.collections.lock().unwrap();
        let Some(handle) = collections.get(name) else {
            return Err(Error::NotExist(format!("collection '{name}'")));
        };

        handle.shutdown();
        collections.remove(name);
        Ok(())
    }
}
