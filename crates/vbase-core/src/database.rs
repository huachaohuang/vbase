use std::collections::HashMap;
use std::fmt;
use std::io::ErrorKind;

use log::info;
use vbase_file::error::Context;
use vbase_util::sync::Arc;
use vbase_util::sync::Mutex;

use crate::Builder;
use crate::Error;
use crate::Result;
use crate::engine::Engine;
use crate::engine::Handle;
use crate::file::RootDir;
use crate::manifest::Desc;
use crate::manifest::EngineDesc;
use crate::options::Options;

pub struct Database {
    root: RootDir,
    options: Options,

    engines: Mutex<HashMap<String, Arc<dyn Handle>>>,
}

impl Database {
    pub(crate) fn open(path: &str, mut builder: Builder) -> Result<Self> {
        let options = builder.options;
        info!("open {path} with {options:#?}");

        // Open or create `path`.
        let dir = match options.env.open_dir(path) {
            Ok(dir) => dir,
            Err(e) if e.kind() != ErrorKind::NotFound => {
                return e.context(|| format!("open {path}"))?;
            }
            Err(_) if builder.error_if_not_exist => {
                return Err(Error::NotExist(path.into()));
            }
            Err(_) => options
                .env
                .create_dir(path)
                .context(|| format!("create {path}"))?,
        };
        let root = RootDir::lock(dir, path.into())?;
        let list = root.list()?;

        // Read the manifest file.
        let mut desc = match root.read_manifest()? {
            Some(_) if builder.error_if_exists => {
                return Err(Error::Exists(format!("manifest at {path}")));
            }
            Some(desc) => desc,
            None if builder.error_if_not_exist => {
                return Err(Error::NotExist(format!("manifest at {path}")));
            }
            None => Desc::default(),
        };

        // Clean up uncommitted engines.
        for id in list.engines {
            if !desc.engines.iter().any(|e| e.id == id) {
                root.delete_engine(id)?;
            }
        }

        // Validate engines in the builder.
        for engine in &desc.engines {
            if !builder.engines.contains_key(&engine.name) {
                return Err(Error::InvalidArgument(format!(
                    "engine {} exists but not provided",
                    engine.name,
                )));
            }
        }

        // Open or create engines in the builder.
        let mut engines = HashMap::new();
        for (name, open) in builder.engines.drain() {
            let handle = match desc.engines.iter().find(|e| e.name == name) {
                Some(engine) => {
                    info!("open {engine:?}");
                    let dir = root.open_engine(engine.id)?;
                    open(engine.id, dir)?
                }
                None => {
                    let engine = EngineDesc {
                        id: desc.last_id + 1,
                        name: name.clone(),
                    };
                    info!("create {engine:?}");
                    let dir = root.create_engine(engine.id)?;
                    let handle = open(engine.id, dir)?;
                    desc.last_id = engine.id;
                    desc.engines.push(engine);
                    handle
                }
            };
            engines.insert(name, handle);
        }

        // Update the manifest to commit created engines.
        root.update_manifest(&desc)?;

        Ok(Self {
            root,
            options,
            engines: Mutex::new(engines),
        })
    }

    pub fn collection<E: Engine>(&self, name: &str) -> Result<E::Collection> {
        let engines = self.engines.lock().unwrap();
        let Some(engine) = engines.get(E::NAME) else {
            return Err(Error::InvalidArgument(format!(
                "engine {} does not exist",
                E::NAME
            )));
        };

        let collection = engine.collection(name)?;
        E::collection(collection)
    }

    pub fn create_collection<E: Engine>(&self, name: &str) -> Result<E::Collection> {
        let engines = self.engines.lock().unwrap();
        let Some(engine) = engines.get(E::NAME) else {
            return Err(Error::InvalidArgument(format!(
                "engine {} does not exist",
                E::NAME
            )));
        };

        info!("create collection {} in engine {}", name, E::NAME);
        let collection = engine.create_collection(name)?;
        E::collection(collection)
    }

    pub fn delete_collection<E: Engine>(&self, name: &str) -> Result<()> {
        let engines = self.engines.lock().unwrap();
        let Some(engine) = engines.get(E::NAME) else {
            return Err(Error::InvalidArgument(format!(
                "engine {} does not exist",
                E::NAME
            )));
        };

        info!("delete collection {} from engine {}", name, E::NAME);
        engine.delete_collection(name)
    }
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("path", &self.root)
            .field("options", &self.options)
            .finish()
    }
}
