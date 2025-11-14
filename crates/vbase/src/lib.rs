mod error;
pub use error::{Error, Result};

mod database;
pub use database::{Builder, Database, Options};

pub mod collections;

mod root;
