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
use crate::options::Options;

pub struct Database {
    root: RootDir,
    options: Options,

    engines: Mutex<HashMap<String, Arc<dyn Handle>>>,
}

impl Database {
    pub(crate) fn open(path: &str, mut builder: Builder) -> Result<Self> {
        let options = builder.options;

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

        // Check engine options.
        for name in &list.engines {
            if !builder.engines.contains_key(name) {
                return Err(Error::InvalidArgument(format!(
                    "engine {name} exists but not provided"
                )));
            }
        }

        // Open or create engines.
        let mut engines = HashMap::new();
        for (name, open) in builder.engines.drain() {
            let dir = root.create_engine(&name)?;
            let handle = open(dir)?;
            engines.insert(name, handle);
        }

        Ok(Self {
            root,
            options,
            engines: Mutex::new(engines),
        })
    }

    pub fn collection<E: Engine>(&self, name: &str) -> Result<E::Collection> {
        let engines = self.engines.lock().unwrap();
        let Some(engine) = engines.get(E::NAME) else {
            return Err(Error::NotExist(format!("engine {}", E::NAME)));
        };

        let collection = engine.collection(name)?;
        Ok(E::collection(collection))
    }

    pub fn create_collection<E: Engine>(&self, name: &str) -> Result<E::Collection> {
        let engines = self.engines.lock().unwrap();
        let Some(engine) = engines.get(E::NAME) else {
            return Err(Error::NotExist(format!("engine {}", E::NAME)));
        };

        info!("create collection {} in engine {}", name, E::NAME);
        let collection = engine.create_collection(name)?;
        Ok(E::collection(collection))
    }

    pub fn delete_collection<E: Engine>(&self, name: &str) -> Result<()> {
        let engines = self.engines.lock().unwrap();
        let Some(engine) = engines.get(E::NAME) else {
            return Err(Error::NotExist(format!("engine {}", E::NAME)));
        };

        info!("delete collection {} from engine {}", name, E::NAME);
        engine.delete_collection(name)?;
        Ok(())
    }
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Core")
            .field("path", &self.root.path())
            .field("options", &self.options)
            .finish()
    }
}
