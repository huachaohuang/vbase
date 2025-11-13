use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use vbase_env::Dir;

use crate::collections::private::Handle as CollectionHandle;
use crate::collections::{Collection, Options as CollectionOptions};
use crate::error::{Error, Result};

#[derive(Clone)]
pub struct Database(Arc<DatabaseHandle>);

impl Database {
    pub fn create_collection<C>(&self, name: &str, options: C::Options) -> Result<C>
    where
        C: Collection,
    {
        let handle = self.0.create_collection(name, options.into())?;
        C::open(self.clone(), handle)
    }

    pub fn delete_collection(&self, name: &str) -> Result<()> {
        self.0.delete_collection(name)
    }
}

struct DatabaseHandle {
    dir: Box<dyn Dir>,
    collections: Mutex<HashMap<String, CollectionHandle>>,
}

impl DatabaseHandle {
    fn create_collection(
        &self,
        name: &str,
        options: CollectionOptions,
    ) -> Result<CollectionHandle> {
        let mut collections = self.collections.lock().unwrap();
        if collections.contains_key(name) {
            return Err(Error::CollectionExist(name.into()));
        }

        let handle = CollectionHandle::open(todo!(), options)?;
        collections.insert(name.into(), handle.clone());
        Ok(handle)
    }

    fn delete_collection(&self, name: &str) -> Result<()> {
        let mut collections = self.collections.lock().unwrap();
        if !collections.contains_key(name) {
            return Err(Error::CollectionNotExist(name.into()));
        }

        collections.remove(name);
        Ok(())
    }
}
