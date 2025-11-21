use std::io;

use thiserror::Error;

/// Errors for database operations.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Error {
    #[error("{context}: {source}")]
    Io { source: io::Error, context: String },
    #[error("{name} is corrupted: {message}")]
    Corrupted { name: String, message: String },
    #[error("{0} already exists")]
    Exists(String),
    #[error("{0} does not exist")]
    NotExist(String),
    #[error("{0}")]
    InvalidArgument(String),
}

#[doc(hidden)]
impl From<vbase_file::Error> for Error {
    fn from(e: vbase_file::Error) -> Self {
        use vbase_file::Error as E;
        match e {
            E::Io { source, context } => Self::Io { source, context },
            E::Corrupted { name, message } => Self::Corrupted { name, message },
        }
    }
}

/// A specialized [`std::result::Result`] for database operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;
