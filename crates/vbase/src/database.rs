use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};

use prost::Message;
use vbase_env::{Env, LocalEnv};
use vbase_file::{JournalFile, JournalFileWriter};

use crate::collections::private::Handle as CollectionHandle;
use crate::collections::{Collection, Options as CollectionOptions};
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
}

impl Builder {
    /// Creates a default builder.
    pub fn new() -> Self {
        Self {
            error_if_exists: false,
            error_if_not_exist: false,
            collection_options: HashMap::new(),
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
    manifest: Mutex<Manifest>,
    collections: Mutex<HashMap<u64, CollectionHandle>>,
    collection_names: Mutex<HashMap<String, u64>>,
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

        let dir = RootDir::lock(dir, path.into())?;
        let mut desc = match dir.read_current()? {
            Some(_) if builder.error_if_exists => {
                return Err(Error::AlreadyExists(format!("database '{path}'")));
            }
            Some(id) => {
                let file = dir.open_manifest(id)?;
                Manifest::load(file)?
            }
            None if builder.error_if_not_exist => {
                return Err(Error::NotExist(format!("database '{path}'")));
            }
            None => Desc::default(),
        };

        let mut collections = HashMap::new();
        let mut collection_names = HashMap::new();
        for desc in desc.collections.values() {
            let dir = dir.open_collection(desc.id)?;
            let Some(options) = builder.collection_options.remove(&desc.name) else {
                return Err(Error::InvalidArgument(format!(
                    "missing options for collection '{}'",
                    desc.name
                )));
            };
            let handle = CollectionHandle::open(dir, options)?;
            collections.insert(desc.id, handle);
            collection_names.insert(desc.name.clone(), desc.id);
        }
        if !builder.collection_options.is_empty() {
            let names = builder
                .collection_options
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join("', '");
            return Err(Error::InvalidArgument(format!(
                "collections [{names}] do not exist"
            )));
        }

        let last_id = desc.last_id + 1;
        let manifest = dir.create_manifest(last_id).and_then(|file| {
            desc.last_id = last_id;
            Manifest::open(desc, file)
        })?;
        dir.switch_current(last_id)?;

        Ok(Self {
            dir,
            next_id: AtomicU64::new(last_id + 1),
            manifest: Mutex::new(manifest),
            collections: Mutex::new(collections),
            collection_names: Mutex::new(collection_names),
        })
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Relaxed)
    }

    fn update_manifest(&self, edit: Edit) -> Result<()> {
        let mut manifest = self.manifest.lock().unwrap();
        if manifest.should_switch_file() {
            let id = self.next_id();
            let file = self.dir.create_manifest(id)?;
            let mut edit = Edit::default();
            edit.last_id = id;
            manifest.switch_file(edit, file)?;
            self.dir.switch_current(id)?;
        }
        manifest.update(edit)
    }

    fn create_collection(
        &self,
        name: &str,
        options: CollectionOptions,
    ) -> Result<CollectionHandle> {
        let mut names = self.collection_names.lock().unwrap();
        if names.contains_key(name) {
            return Err(Error::AlreadyExists(format!("collection '{name}'")));
        }
        let mut collections = self.collections.lock().unwrap();

        let id = self.next_id();
        let dir = self.dir.create_collection(id)?;
        let handle = CollectionHandle::open(dir, options)?;

        let desc = CollectionDesc {
            id,
            name: name.into(),
            kind: handle.kind() as u32,
        };
        let mut edit = Edit::default();
        edit.last_id = id;
        edit.add_collections.push(desc);
        self.update_manifest(edit)?;

        names.insert(name.into(), id);
        collections.insert(id, handle.clone());
        Ok(handle)
    }

    fn delete_collection(&self, name: &str) -> Result<()> {
        let mut names = self.collection_names.lock().unwrap();
        let Some(id) = names.get(name).cloned() else {
            return Err(Error::NotExist(format!("collection '{name}'")));
        };
        let mut collections = self.collections.lock().unwrap();

        let mut edit = Edit::default();
        edit.delete_collections.push(id);
        self.update_manifest(edit)?;

        names.remove(name);
        let handle = collections.remove(&id).unwrap();
        handle.shutdown();
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
