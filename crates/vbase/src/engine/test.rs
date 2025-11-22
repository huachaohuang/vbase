use std::collections::HashMap;

use vbase_core::engine;
use vbase_env::Dir;
use vbase_util::sync::Arc;
use vbase_util::sync::Mutex;

use crate::Error;
use crate::Result;

const NAME: &str = "Test";

/// A test engine.
pub(crate) struct Engine;

impl engine::Engine for Engine {
    type Handle = Handle;
    type Collection = Collection;

    const NAME: &str = NAME;

    fn open(_: u64, _: Box<dyn Dir>) -> Result<Self::Handle> {
        Ok(Handle::default())
    }
}

#[derive(Default)]
pub(crate) struct Handle {
    id: u64,
    collections: Mutex<HashMap<String, Arc<CollectionHandle>>>,
}

impl engine::Handle for Handle {
    fn id(&self) -> u64 {
        self.id
    }

    fn name(&self) -> &str {
        NAME
    }

    fn write(&self, lsn: u64, batch: &[u8]) {
        todo!()
    }

    fn last_lsn(&self) -> u64 {
        0
    }

    fn collection(&self, name: &str) -> Result<Arc<dyn engine::CollectionHandle>> {
        let collections = self.collections.lock().unwrap();
        let Some(collection) = collections.get(name) else {
            return Err(Error::NotExist(format!("collection {name}")));
        };
        Ok(collection.clone())
    }

    fn create_collection(&self, name: &str) -> Result<Arc<dyn engine::CollectionHandle>> {
        let mut collections = self.collections.lock().unwrap();
        if collections.contains_key(name) {
            return Err(Error::Exists(format!("collection {name}")));
        }
        let collection = Arc::new(CollectionHandle);
        collections.insert(name.into(), collection.clone());
        Ok(collection)
    }

    fn delete_collection(&self, name: &str) -> Result<()> {
        let mut collections = self.collections.lock().unwrap();
        if collections.remove(name).is_none() {
            return Err(Error::NotExist(format!("collection {name}")));
        }
        Ok(())
    }
}

/// A test collection.
#[derive(Debug)]
pub(crate) struct Collection {
    handle: Arc<CollectionHandle>,
}

impl engine::Collection for Collection {
    type Handle = CollectionHandle;

    fn open(handle: Arc<Self::Handle>) -> Self {
        Self { handle }
    }
}

#[derive(Debug)]
pub(crate) struct CollectionHandle;

impl engine::CollectionHandle for CollectionHandle {}
