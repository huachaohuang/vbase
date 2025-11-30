use std::any::Any;

use vbase_env::boxed::Dir;
use vbase_util::sync::Arc;

use crate::Result;

/// A database engine.
pub trait Engine {
    type Handle: EngineHandle;

    /// The name of the engine.
    const NAME: &str;

    /// Opens a handle to the engine.
    fn open(id: u64, dir: Dir) -> Result<Self::Handle>;
}

/// A handle to an opened engine.
pub trait EngineHandle: Send + Sync + 'static {
    /// Returns the id of the engine.
    fn id(&self) -> u64;

    /// Returns the name of the engine.
    fn name(&self) -> &str;

    /// Writes a batch with the given LSN.
    ///
    /// Writes to a deleted bucket should be ignored.
    fn write(&self, lsn: u64, batch: &[u8]);

    /// Returns the last LSN written to the engine.
    fn last_lsn(&self) -> u64;

    /// Returns a bucket if it exists.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotExist`] if the bucket does not exist.
    fn bucket(&self, name: &str) -> Result<Arc<dyn BucketHandle>>;

    /// Creates a bucket.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Exists`] if the bucket already exists.
    fn create_bucket(&self, name: &str) -> Result<Arc<dyn BucketHandle>>;

    /// Deletes a bucket.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::NotExist`] if the bucket does not exist.
    fn delete_bucket(&self, name: &str) -> Result<()>;
}

/// A bucket in the engine.
pub trait Bucket {
    type Handle: BucketHandle;

    /// Opens a bucket with the given handle.
    fn open(handle: Arc<Self::Handle>) -> Self;

    /// Returns the handle of the bucket.
    fn handle(&self) -> &Self::Handle;
}

/// A handle to an opened bucket.
pub trait BucketHandle: Any + Send + Sync + 'static {
    /// Returns the id of the bucket.
    fn id(&self) -> u64;

    /// Returns the engine id of the bucket.
    fn engine_id(&self) -> u64;
}

/// A reader associated with a bucket.
pub trait Reader<'a> {
    /// Creates a reader for the given bucket.
    fn new(id: u64) -> Self;
}

/// A writer associated with a bucket.
pub trait Writer<'a> {
    /// Creates a writer for the given bucket.
    fn new(id: u64, buf: &'a mut Vec<u8>) -> Self;
}
