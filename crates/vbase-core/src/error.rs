use std::io;

use thiserror::Error;

/// Errors for database operations.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{name} is corrupted: {message}")]
    Corrupted { name: String, message: String },
    #[error("{0} is locked")]
    Locked(String),
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
            E::Io(e) => Error::Io(e),
            E::Corrupted { name, message } => Error::Corrupted { name, message },
        }
    }
}

/// A specialized [`std::result::Result`] for database operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An extension to create a result with [`Error::Corrupted`].
pub trait Corrupted<T> {
    fn corrupted<M>(&self, message: M) -> Result<T>
    where
        M: Into<String>;
}

impl<T> Corrupted<T> for str {
    fn corrupted<M>(&self, message: M) -> Result<T>
    where
        M: Into<String>,
    {
        Err(Error::Corrupted {
            name: self.into(),
            message: message.into(),
        })
    }
}

impl<T> Corrupted<T> for String {
    fn corrupted<M>(&self, message: M) -> Result<T>
    where
        M: Into<String>,
    {
        self.as_str().corrupted(message)
    }
}
