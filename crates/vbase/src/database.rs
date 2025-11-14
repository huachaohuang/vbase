use std::collections::{self, HashMap};
use std::io::ErrorKind;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};

use prost::Message;
use vbase_env::{Env, LocalEnv};
use vbase_file::{JournalFile, JournalFileWriter};

use crate::collections::private::Handle as CollectionHandle;
use crate::collections::{
    Collection, CollectionInfo, Kind as CollectionKind, Options as CollectionOptions,
};
use crate::error::{Error, Result};
use crate::manifest::{CollectionDesc, Desc, Edit};
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

    collection_options: HashMap<String, CollectionOptions>,
    error_if_collection_exists: bool,
    error_if_collection_not_exist: bool,
}

impl Builder {
    /// Creates a default builder.
    pub fn new() -> Self {
        Self {
            error_if_exists: false,
            error_if_not_exist: false,
            collection_options: HashMap::new(),
            error_if_collection_exists: false,
            error_if_collection_not_exist: false,
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

    /// Sets the options for a collection.
    pub fn collection_options<N, O>(mut self, name: N, options: O) -> Self
    where
        N: Into<String>,
        O: Into<CollectionOptions>,
    {
        self.collection_options.insert(name.into(), options.into());
        self
    }

    /// If enabled, returns an error if the collection already exists.
    ///
    /// Default: false
    pub fn error_if_collection_exists(mut self, enable: bool) -> Self {
        self.error_if_collection_exists = enable;
        self
    }

    /// If enabled, returns an error if the collection does not exist.
    ///
    /// Default: false
    pub fn error_if_collection_not_exist(mut self, enable: bool) -> Self {
        self.error_if_collection_not_exist = enable;
        self
    }

    /// Opens a database.
    ///
    /// The builder creates the database and collections if they do not exist by
    /// default. This behavior can be changed by [`Self::error_if_exists`],
    /// [`Self::error_if_not_exist`], [`Self::error_if_collection_exists`],
    /// and [`Self::error_if_collection_not_exist`].
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

/// A multi-model embedded database.
#[derive(Clone)]
pub struct Database(Arc<DatabaseHandle>);

impl Database {
    /// Opens a database.
    ///
    /// This function creates the database if it does not exist.
    ///
    /// This is equivalent to `Builder::new().open(path, options)`.
    /// See [`Builder::open`] for more details.
    pub fn open(path: &str, options: Options) -> Result<Self> {
        Builder::new().open(path, options)
    }

    /// Lists collections in the database.
    pub fn list(path: &str, options: Options) -> Result<Vec<CollectionInfo>> {
        DatabaseHandle::list(path, options)
    }

    /// Returns a collection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotExist`] if `name` does not exist.
    pub fn collection<C>(&self, name: &str) -> Result<C>
    where
        C: Collection,
    {
        let handle = self.0.collection(name)?;
        C::open(self.clone(), handle)
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
    root: RootDir,
    next_id: AtomicU64,
    manifest: Mutex<Manifest>,
    collections: Mutex<HashMap<String, CollectionHandle>>,
}

impl DatabaseHandle {
    fn open(path: &str, options: Options, mut builder: Builder) -> Result<Self> {
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

        let root = RootDir::lock(dir, path.into())?;
        let mut desc = match root.read_current()? {
            Some(_) if builder.error_if_exists => {
                return Err(Error::AlreadyExists(format!("database '{path}'")));
            }
            Some(id) => {
                let file = root.open_manifest(id)?;
                Manifest::load(file)?
            }
            None if builder.error_if_not_exist => {
                return Err(Error::NotExist(format!("database '{path}'")));
            }
            None => Desc::default(),
        };

        let mut edit = Edit::default();
        let mut last_id = desc.last_id;
        let mut collections = HashMap::new();
        for (name, options) in builder.collection_options.drain() {
            let handle = match desc.collections.values().find(|c| c.name == name) {
                Some(_) if builder.error_if_collection_exists => {
                    return Err(Error::AlreadyExists(format!("collection '{name}'")));
                }
                Some(desc) if desc.kind != options.kind() as u32 => {
                    return Err(Error::InvalidArgument(format!(
                        "collection '{name}' is a {:?}, not a {:?}",
                        CollectionKind::from(desc.kind),
                        options.kind()
                    )));
                }
                Some(desc) => root
                    .open_collection(desc.id)
                    .and_then(|dir| CollectionHandle::open(dir, options))?,
                None if builder.error_if_collection_not_exist => {
                    return Err(Error::NotExist(format!("collection '{name}'")));
                }
                None => {
                    last_id += 1;
                    let desc = CollectionDesc {
                        id: last_id,
                        name: name.clone(),
                        kind: options.kind() as u32,
                    };
                    edit.add_collections.push(desc);
                    root.create_collection(last_id)
                        .and_then(|dir| CollectionHandle::open(dir, options))?
                }
            };
            collections.insert(name, handle);
        }

        last_id += 1;
        let manifest = root.create_manifest(last_id).and_then(|file| {
            desc.last_id = last_id;
            Manifest::open(desc, file)
        })?;
        root.switch_current(last_id)?;

        Ok(Self {
            root,
            next_id: AtomicU64::new(last_id + 1),
            manifest: Mutex::new(manifest),
            collections: Mutex::new(collections),
        })
    }

    fn list(path: &str, options: Options) -> Result<Vec<CollectionInfo>> {
        let dir = options
            .env
            .open_dir(path)
            .map_err(|e| Error::io(e, format!("open database '{path}'")))?;
        let root = RootDir::new(dir, path.into());

        let id = root.read_current()?.ok_or_else(|| {
            Error::io(ErrorKind::NotFound.into(), format!("read current manifest"))
        })?;
        let desc = root.open_manifest(id).and_then(Manifest::load)?;
        let list = desc
            .collections
            .into_values()
            .map(|c| CollectionInfo {
                id: c.id,
                name: c.name,
                kind: CollectionKind::from(c.kind),
            })
            .collect();
        Ok(list)
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Relaxed)
    }

    fn update_manifest(&self, edit: Edit) -> Result<()> {
        let mut manifest = self.manifest.lock().unwrap();
        if manifest.should_switch_file() {
            let id = self.next_id();
            let file = self.root.create_manifest(id)?;
            let mut edit = Edit::default();
            edit.last_id = id;
            manifest.switch_file(edit, file)?;
            self.root.switch_current(id)?;
        }
        manifest.update(edit)
    }

    fn collection(&self, name: &str) -> Result<CollectionHandle> {
        let collections = self.collections.lock().unwrap();
        let handle = collections
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotExist(format!("collection '{name}'")))?;
        Ok(handle)
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

        let id = self.next_id();
        let dir = self.root.create_collection(id)?;
        let desc = CollectionDesc {
            id,
            name: name.into(),
            kind: options.kind() as u32,
        };
        let handle = CollectionHandle::open(dir, options)?;

        let mut edit = Edit::default();
        edit.last_id = id;
        edit.add_collections.push(desc);
        self.update_manifest(edit)?;

        collections.insert(name.into(), handle.clone());
        Ok(handle)
    }

    fn delete_collection(&self, name: &str) -> Result<()> {
        let mut collections = self.collections.lock().unwrap();
        let Some(handle) = collections.get(name) else {
            return Err(Error::NotExist(format!("collection '{name}'")));
        };

        let mut edit = Edit::default();
        edit.delete_collections.push(name.into());
        self.update_manifest(edit)?;

        handle.shutdown();
        collections.remove(name);
        Ok(())
    }
}

struct Manifest {
    desc: Desc,
    file: JournalFileWriter,
    /// The initial size of the current file.
    ///
    /// This is used to determine when to switch to a new file.
    init_size: u64,
}

impl Manifest {
    const MIN_FILE_SIZE: u64 = 1024 * 1024;

    fn load(mut file: JournalFile) -> Result<Desc> {
        let mut desc = Desc::default();
        while let Some(data) = file.read()? {
            let edit =
                Edit::decode(data).map_err(|e| Error::corrupted(file.name(), format!("{e}")))?;
            desc.merge(edit);
        }
        Ok(desc)
    }

    fn open(desc: Desc, file: JournalFileWriter) -> Result<Self> {
        let mut this = Self {
            desc,
            file,
            init_size: 0,
        };
        this.init_file()?;
        Ok(this)
    }

    /// Updates the manifest with an edit.
    fn update(&mut self, edit: Edit) -> Result<()> {
        self.file.write(edit.encode_to_vec())?;
        self.file.sync()?;
        self.desc.merge(edit);
        Ok(())
    }

    /// Initializes the current file.
    fn init_file(&mut self) -> Result<()> {
        let edit = self.desc.to_edit();
        self.file.write(edit.encode_to_vec())?;
        self.file.sync()?;
        self.init_size = self.file.size();
        Ok(())
    }

    /// Updates the manifest and switches to a new file.
    fn switch_file(&mut self, edit: Edit, file: JournalFileWriter) -> Result<()> {
        self.desc.merge(edit);
        self.file = file;
        self.init_file()
    }

    /// Returns true if the current file should be switched.
    fn should_switch_file(&self) -> bool {
        self.file.size() >= (self.init_size * 2).max(Self::MIN_FILE_SIZE)
    }
}
