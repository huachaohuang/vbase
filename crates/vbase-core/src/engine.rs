use std::any::Any;

use vbase_env::Dir;
use vbase_util::sync::Arc;

use crate::Result;

pub trait Engine {
    type Collection;

    const NAME: &'static str;

    fn open(id: u64, dir: Box<dyn Dir>) -> Result<Arc<dyn Handle>>;

    fn collection(handle: Arc<dyn CollectionHandle>) -> Result<Self::Collection>;
}

pub trait Handle {
    fn collection(&self, name: &str) -> Result<Arc<dyn CollectionHandle>>;

    fn create_collection(&self, name: &str) -> Result<Arc<dyn CollectionHandle>>;

    fn delete_collection(&self, name: &str) -> Result<()>;
}

pub trait CollectionHandle: Any + Send + Sync {}
