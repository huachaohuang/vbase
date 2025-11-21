use std::collections::HashMap;

use vbase_util::sync::Arc;
use vbase_util::sync::Mutex;

use crate::Builder;
use crate::Error;
use crate::Result;
use crate::engine::Engine;
use crate::engine::Handle;

pub struct Database {
    engines: Mutex<HashMap<String, Arc<dyn Handle>>>,
}

impl Database {
    pub(crate) fn open(path: &str, builder: Builder) -> Result<Self> {
        let mut engines = HashMap::new();
        for (name, open) in builder.engines {
            let handle = open()?;
            engines.insert(name, handle);
        }
        Ok(Self {
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

        let collection = engine.create_collection(name)?;
        Ok(E::collection(collection))
    }

    pub fn delete_collection<E: Engine>(&self, name: &str) -> Result<()> {
        let engines = self.engines.lock().unwrap();
        let Some(engine) = engines.get(E::NAME) else {
            return Err(Error::NotExist(format!("engine {}", E::NAME)));
        };

        engine.delete_collection(name)?;
        Ok(())
    }
}
