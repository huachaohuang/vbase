//! Traits and implementations to interact with the system environment.

use std::io::ErrorKind;
use std::io::Result;

#[cfg(feature = "test")]
mod test;
#[cfg(feature = "test")]
pub use test::TestDir;
#[cfg(feature = "test")]
pub use test::TestEnv;

mod mock;
pub use mock::MockDir;
pub use mock::MockEnv;

mod local;
pub use local::LocalDir;
pub use local::LocalEnv;

pub mod boxed;

/// A system environment.
pub trait Env: Send + Sync {
    /// Returns the name of the environment.
    fn name(&self) -> &str;

    /// Opens a directory.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::NotFound`] if `name` does not exist.
    fn open_dir(&self, name: &str) -> Result<Box<dyn Dir>>;

    /// Creates a directory if it does not exist.
    ///
    /// This function creates parent directories as needed.
    ///
    /// Returns the original directory if `name` already exists.
    fn create_dir(&self, name: &str) -> Result<Box<dyn Dir>>;

    /// Deletes a directory, after deleting all its entries.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::NotFound`] if `name` does not exist.
    fn delete_dir(&self, name: &str) -> Result<()>;
}

/// A directory in the environment.
pub trait Dir: Send + Sync {
    /// Returns the names of all entries.
    fn list(&self) -> Result<Vec<String>>;

    /// Opens a directory.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::NotFound`] if `name` does not exist.
    fn open_dir(&self, name: &str) -> Result<Box<dyn Dir>>;

    /// Creates a directory if it does not exist.
    ///
    /// This function creates parent directories as needed.
    ///
    /// Returns the original directory if `name` already exists.
    fn create_dir(&self, name: &str) -> Result<Box<dyn Dir>>;

    /// Deletes a directory, after deleting all its entries.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::NotFound`] if `name` does not exist.
    fn delete_dir(&self, name: &str) -> Result<()>;

    /// Locks a file.
    ///
    /// This function creates a new file if `name` does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::WouldBlock`] if `name` is already locked.
    fn lock_file(&self, name: &str) -> Result<Box<dyn LockedFile>>;

    /// Reads all data from a file.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::NotFound`] if `name` does not exist.
    fn read_file(&self, name: &str) -> Result<Vec<u8>>;

    /// Writes `data` as the entire content of a file.
    ///
    /// This function creates a new file if `name` does not exist.
    fn write_file(&self, name: &str, data: &[u8]) -> Result<()>;

    /// Deletes a file.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::NotFound`] if `name` does not exist.
    fn delete_file(&self, name: &str) -> Result<()>;

    /// Renames a file.
    ///
    /// This function replaces the original file if `to` already exists.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::NotFound`] if `from` does not exist.
    fn rename_file(&self, from: &str, to: &str) -> Result<()>;

    /// Opens a file for positional reads.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::NotFound`] if `name` does not exist.
    fn open_positional_file(&self, name: &str) -> Result<Box<dyn PositionalFile>>;

    /// Opens a file for sequential reads.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::NotFound`] if `name` does not exist.
    fn open_sequential_file(&self, name: &str) -> Result<Box<dyn SequentialFile>>;

    /// Creates a file for sequential writes.
    ///
    /// This function truncates the original file if `name` already exists.
    fn create_sequential_file(&self, name: &str) -> Result<Box<dyn SequentialFileWriter>>;
}

/// A locked file.
///
/// Dropping the locked file unlocks it.
pub trait LockedFile: Send + Sync {}

/// A file opened for positional reads.
pub trait PositionalFile: Send + Sync {
    /// Reads some bytes into `buf` from the file `offset`.
    ///
    /// Returns the number of bytes read.
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// Reads the exact number of bytes to fill `buf` from the file `offset`.
    fn read_exact(&self, buf: &mut [u8], mut offset: u64) -> Result<()> {
        let mut len = 0;
        while len < buf.len() {
            match self.read(&mut buf[len..], offset) {
                Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
                Ok(n) => {
                    len += n;
                    offset += n as u64;
                }
                Err(err) if err.kind() == ErrorKind::Interrupted => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }
}

impl PositionalFile for Box<dyn PositionalFile> {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        (**self).read(buf, offset)
    }
}

/// A file opened for sequential reads.
pub trait SequentialFile: Send + Sync {
    /// Reads some bytes into `buf` from the file.
    ///
    /// Returns the number of bytes read.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Reads the exact number of bytes to fill `buf` from the file.
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut len = 0;
        while len < buf.len() {
            match self.read(&mut buf[len..]) {
                Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
                Ok(n) => len += n,
                Err(err) if err.kind() == ErrorKind::Interrupted => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// Reads available bytes to fill `buf` from the file until EOF.
    ///
    /// Returns the number of bytes read.
    fn read_until_end(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut len = 0;
        while len < buf.len() {
            match self.read(&mut buf[len..]) {
                Ok(0) => return Ok(len),
                Ok(n) => len += n,
                Err(err) if err.kind() == ErrorKind::Interrupted => {}
                Err(err) => return Err(err),
            }
        }
        Ok(len)
    }

    /// Returns the current file offset.
    fn offset(&self) -> u64;
}

impl SequentialFile for Box<dyn SequentialFile> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (**self).read(buf)
    }

    fn offset(&self) -> u64 {
        (**self).offset()
    }
}

/// A file opened for sequential writes.
pub trait SequentialFileWriter: Send + Sync {
    /// Synchronizes all data to the file.
    fn sync(&mut self) -> Result<()>;

    /// Writes some bytes from `buf` to the file.
    ///
    /// Returns the number of bytes written.
    fn write(&mut self, buf: &[u8]) -> Result<usize>;

    /// Writes the exact number of bytes from `buf` to the file.
    fn write_exact(&mut self, buf: &[u8]) -> Result<()> {
        let mut len = 0;
        while len < buf.len() {
            match self.write(&buf[len..]) {
                Ok(0) => return Err(ErrorKind::WriteZero.into()),
                Ok(n) => len += n,
                Err(err) if err.kind() == ErrorKind::Interrupted => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// Returns the current file offset.
    fn offset(&self) -> u64;
}

impl SequentialFileWriter for Box<dyn SequentialFileWriter> {
    fn sync(&mut self) -> Result<()> {
        (**self).sync()
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        (**self).write(buf)
    }

    fn offset(&self) -> u64 {
        (**self).offset()
    }
}
