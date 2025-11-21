use std::io;

use thiserror::Error;

/// Errors for file operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("{context}: {source}")]
    Io { source: io::Error, context: String },
    #[error("{name} is corrupted: {message}")]
    Corrupted { name: String, message: String },
}

/// A specialized [`std::result::Result`] for file operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// An extension to create a result with context.
pub trait Context<T> {
    fn context<F, C>(self, context: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: Into<String>;
}

impl<T> Context<T> for io::Error {
    fn context<F, C>(self, context: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: Into<String>,
    {
        Err(Error::Io {
            source: self,
            context: context().into(),
        })
    }
}

impl<T> Context<T> for Result<T, io::Error> {
    fn context<F, C>(self, context: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: Into<String>,
    {
        match self {
            Ok(x) => Ok(x),
            Err(e) => e.context(context),
        }
    }
}

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
