use vbase_util::sync::Arc;

use crate::Result;

pub trait Engine {
    type Handle;
    type Collection;

    const NAME: &'static str;

    fn open() -> Result<Self::Handle>;

    fn collection(handle: Arc<dyn CollectionHandle>) -> Self::Collection;
}

pub trait Handle {
    fn collection(&self, name: &str) -> Result<Arc<dyn CollectionHandle>>;

    fn create_collection(&self, name: &str) -> Result<Arc<dyn CollectionHandle>>;

    fn delete_collection(&self, name: &str) -> Result<()>;
}

pub trait CollectionHandle {}
