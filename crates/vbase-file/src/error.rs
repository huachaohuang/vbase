use std::io;

use thiserror::Error;

/// Errors for file operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("{context}: {source}")]
    Io { source: io::Error, context: String },
    #[error("{name} is corrupted: {message}")]
    Corrupted { name: String, message: String },
    #[error("{0}")]
    InvalidArgument(String),
}

impl Error {
    pub fn io<C>(source: io::Error, context: C) -> Self
    where
        C: Into<String>,
    {
        Self::Io {
            source,
            context: context.into(),
        }
    }

    pub fn corrupted<N, M>(name: N, message: M) -> Self
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

/// A specialized [`std::result::Result`] for file operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;
