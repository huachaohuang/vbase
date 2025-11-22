use std::any::Any;

use vbase_env::Dir;
use vbase_util::sync::Arc;

use crate::Result;

/// A database engine.
pub trait Engine {
    #[doc(hidden)]
    type Handle: Handle + 'static;
    type Collection: Collection;

    /// The name of the engine.
    const NAME: &str;

    #[doc(hidden)]
    /// Opens a handle to the engine.
    fn open(id: u64, dir: Box<dyn Dir>) -> Result<Self::Handle>;
}

/// A handle to an opened engine.
pub trait Handle: Send + Sync {
    /// Returns the id of the engine.
    fn id(&self) -> u64;

    /// Returns the name of the engine.
    fn name(&self) -> &str;

    /// Writes a batch with the given LSN.
    fn write(&self, lsn: u64, batch: &[u8]);

    /// Returns the last LSN written to the engine.
    fn last_lsn(&self) -> u64;

    /// Gets a collection.
    fn collection(&self, name: &str) -> Result<Arc<dyn CollectionHandle>>;

    /// Creates a collection.
    fn create_collection(&self, name: &str) -> Result<Arc<dyn CollectionHandle>>;

    /// Deletes a collection.
    fn delete_collection(&self, name: &str) -> Result<()>;
}

/// A collection in the engine.
pub trait Collection {
    type Handle: CollectionHandle + 'static;

    fn open(handle: Arc<Self::Handle>) -> Self;
}

/// A handle to an opened collection.
pub trait CollectionHandle: Any + Send + Sync {}
