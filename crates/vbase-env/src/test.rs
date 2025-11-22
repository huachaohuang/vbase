use std::io::Result;
use std::sync::Arc;

use crate::Dir;
use crate::Env;
use crate::LockedFile;
use crate::PositionalFile;
use crate::SequentialFile;
use crate::SequentialFileWriter;

/// An implementation of [`Env`] for tests.
#[derive(Clone)]
pub struct TestEnv {
    root: Arc<TestDir>,
}

impl TestEnv {
    pub fn new() -> Result<Self> {
        let root = TestDir::new()?;
        Ok(Self {
            root: Arc::new(root),
        })
    }
}

impl Env for TestEnv {
    fn name(&self) -> &str {
        "TestEnv"
    }

    fn open_dir(&self, name: &str) -> Result<Box<dyn Dir>> {
        self.root.open_dir(name)
    }

    fn create_dir(&self, name: &str) -> Result<Box<dyn Dir>> {
        self.root.create_dir(name)
    }

    fn delete_dir(&self, name: &str) -> Result<()> {
        self.root.delete_dir(name)
    }
}

/// An implementation of [`Dir`] for miri tests.
///
/// Miri does not support file system operations, so we use [`crate::MockDir`]
/// here.
#[cfg(miri)]
pub struct TestDir {
    dir: Box<dyn Dir>,
}

#[cfg(miri)]
impl TestDir {
    pub fn new() -> Result<Self> {
        Ok(Self {
            dir: Box::new(crate::MockDir::new()),
        })
    }

    fn new_subdir(&self, dir: Box<dyn Dir>) -> Box<dyn Dir> {
        dir
    }
}

/// An implementation of [`Dir`] for non-miri tests.
#[cfg(not(miri))]
pub struct TestDir {
    dir: Box<dyn Dir>,
    temp_dir: std::sync::Arc<tempfile::TempDir>,
}

#[cfg(not(miri))]
impl TestDir {
    pub fn new() -> Result<Self> {
        let temp_dir = tempfile::tempdir()?;
        let local_dir = crate::LocalDir::open(temp_dir.path())?;
        Ok(Self {
            dir: Box::new(local_dir),
            temp_dir: temp_dir.into(),
        })
    }

    fn new_subdir(&self, dir: Box<dyn Dir>) -> Box<dyn Dir> {
        Box::new(Self {
            dir,
            temp_dir: self.temp_dir.clone(),
        })
    }
}

impl Dir for TestDir {
    fn list(&self) -> Result<Vec<String>> {
        self.dir.list()
    }

    fn open_dir(&self, name: &str) -> Result<Box<dyn Dir>> {
        let dir = self.dir.open_dir(name)?;
        Ok(self.new_subdir(dir))
    }

    fn create_dir(&self, name: &str) -> Result<Box<dyn Dir>> {
        let dir = self.dir.create_dir(name)?;
        Ok(self.new_subdir(dir))
    }

    fn delete_dir(&self, name: &str) -> Result<()> {
        self.dir.delete_dir(name)
    }

    fn lock_file(&self, name: &str) -> Result<Box<dyn LockedFile>> {
        self.dir.lock_file(name)
    }

    fn read_file(&self, name: &str) -> Result<Vec<u8>> {
        self.dir.read_file(name)
    }

    fn write_file(&self, name: &str, data: &[u8]) -> Result<()> {
        self.dir.write_file(name, data)
    }

    fn delete_file(&self, name: &str) -> Result<()> {
        self.dir.delete_file(name)
    }

    fn rename_file(&self, from: &str, to: &str) -> Result<()> {
        self.dir.rename_file(from, to)
    }

    fn open_sequential_file(&self, name: &str) -> Result<Box<dyn SequentialFile>> {
        self.dir.open_sequential_file(name)
    }

    fn open_positional_file(&self, name: &str) -> Result<Box<dyn PositionalFile>> {
        self.dir.open_positional_file(name)
    }

    fn create_sequential_file(&self, name: &str) -> Result<Box<dyn SequentialFileWriter>> {
        self.dir.create_sequential_file(name)
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::*;

    #[test]
    fn test_lock_file() -> Result<()> {
        let dir = TestDir::new()?;
        let name = "lock";
        let file = dir.lock_file(name)?;
        assert_eq!(
            dir.lock_file(name).map(|_| ()).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
        drop(file);
        dir.lock_file(name)?;
        Ok(())
    }
}
