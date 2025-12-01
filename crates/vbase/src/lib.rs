mod database;
pub use database::Builder;
pub use database::Database;

mod core {
    pub use vbase_core::Error;
    pub use vbase_core::Result;
    pub use vbase_core::WriteBatch;
    pub use vbase_core::engine::Bucket;
    pub use vbase_core::engine::Engine;
    pub use vbase_core::options::Options;
    pub use vbase_core::options::WriteOptions;
}
pub use core::*;

pub mod tree {
    pub use vbase_tree::Bucket;
    pub use vbase_tree::Engine;
}
