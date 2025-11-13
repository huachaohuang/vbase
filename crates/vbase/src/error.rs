use std::io;

use thiserror::Error;
use vbase_file::Error as FileError;

/// Errors for database operations.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum Error {
    #[error("{context}: {source}")]
    Io { source: io::Error, context: String },
    #[error("{name} is corrupted: {message}")]
    Corrupted { name: String, message: String },
    #[error("{0}")]
    InvalidArgument(String),
    #[error("collection '{0}' exists")]
    CollectionExist(String),
    #[error("collection '{0}' does not exist")]
    CollectionNotExist(String),
}

impl Error {
    pub(crate) fn io<C>(source: io::Error, context: C) -> Self
    where
        C: Into<String>,
    {
        Self::Io {
            source,
            context: context.into(),
        }
    }

    pub(crate) fn corrupted<N, M>(name: N, message: M) -> Self
    where
        N: Into<String>,
        M: Into<String>,
    {
        Self::Corrupted {
            name: name.into(),
            message: message.into(),
        }
    }
}

impl From<FileError> for Error {
    fn from(e: FileError) -> Self {
        match e {
            FileError::Io { source, context } => Self::Io { source, context },
            FileError::Corrupted { name, message } => Self::Corrupted { name, message },
            FileError::InvalidArgument(message) => Self::InvalidArgument(message),
        }
    }
}

/// A specialized [`std::result::Result`] for database operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;
