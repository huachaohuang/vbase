use std::any::Any;
use std::collections::HashMap;

use vbase_core::engine;
use vbase_env::Dir;
use vbase_util::sync::Arc;
use vbase_util::sync::Mutex;

use crate::Error;
use crate::Result;

/// A test engine.
pub(crate) struct Engine;

impl engine::Engine for Engine {
    type Collection = Collection;

    const NAME: &'static str = "Test";

    fn open(_: u64, _: Box<dyn Dir>) -> Result<Arc<dyn engine::Handle>> {
        Ok(Arc::new(Handle::default()))
    }

    fn collection(handle: Arc<dyn engine::CollectionHandle>) -> Result<Collection> {
        let handle = handle as Arc<dyn Any + Send + Sync>;
        let handle = handle.downcast::<CollectionHandle>().map_err(|_| {
            Error::InvalidArgument(format!(
                "invalid collection handle for {} engine",
                Self::NAME
            ))
        })?;
        Ok(Collection { handle })
    }
}

#[derive(Default)]
struct Handle {
    collections: Mutex<HashMap<String, Arc<CollectionHandle>>>,
}

impl engine::Handle for Handle {
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

#[derive(Debug)]
struct CollectionHandle;

impl engine::CollectionHandle for CollectionHandle {}
