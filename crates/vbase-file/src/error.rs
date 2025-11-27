use std::io;

use thiserror::Error;

/// Errors for file operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{name} is corrupted: {message}")]
    Corrupted { name: String, message: String },
}

/// A specialized [`std::result::Result`] for file operations.
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
