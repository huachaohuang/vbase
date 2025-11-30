use std::collections::HashMap;
use std::io::ErrorKind;
use std::io::Result;
use std::sync::Arc;
use std::sync::Mutex;

use crate::Dir;
use crate::Env;
use crate::LockedFile;
use crate::PositionalFile;
use crate::SequentialFile;
use crate::SequentialFileWriter;

/// An implementation of [`Env`] based on a mock file system.
#[derive(Default)]
pub struct MockEnv {
    root: MockDir,
}

impl Env for MockEnv {
    fn name(&self) -> &str {
        "MockEnv"
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

/// An implementation of [`Dir`] based on a mock file system.
#[derive(Default)]
pub struct MockDir(DirHandle);

impl Dir for MockDir {
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.0.list())
    }

    fn open_dir(&self, name: &str) -> Result<Box<dyn Dir>> {
        let dir = self.0.open_dir(name)?;
        Ok(Box::new(Self(dir)))
    }

    fn create_dir(&self, name: &str) -> Result<Box<dyn Dir>> {
        let dir = self.0.create_dir(name)?;
        Ok(Box::new(Self(dir)))
    }

    fn delete_dir(&self, name: &str) -> Result<()> {
        self.0.delete_dir(name)
    }

    fn lock_file(&self, name: &str) -> Result<Box<dyn LockedFile>> {
        let file = self.0.create_file(name)?;
        let lock = MockLockedFile::new(file)?;
        Ok(Box::new(lock))
    }

    fn read_file(&self, name: &str) -> Result<Vec<u8>> {
        let file = self.0.open_file(name)?;
        Ok(file.data())
    }

    fn write_file(&self, name: &str, data: &[u8]) -> Result<()> {
        let file = self.0.create_file(name)?;
        file.write(data, 0);
        Ok(())
    }

    fn delete_file(&self, name: &str) -> Result<()> {
        self.0.delete_file(name)
    }

    fn rename_file(&self, from: &str, to: &str) -> Result<()> {
        self.0.rename_file(from, to)
    }

    fn open_positional_file(&self, name: &str) -> Result<Box<dyn PositionalFile>> {
        let file = self.0.open_file(name)?;
        Ok(Box::new(MockPositionalFile(file)))
    }

    fn open_sequential_file(&self, name: &str) -> Result<Box<dyn SequentialFile>> {
        let file = self.0.open_file(name)?;
        Ok(Box::new(MockSequentialFile::new(file)))
    }

    fn create_sequential_file(&self, name: &str) -> Result<Box<dyn SequentialFileWriter>> {
        let file = self.0.create_file(name)?;
        Ok(Box::new(MockSequentialFile::new(file)))
    }
}

struct MockLockedFile(FileHandle);

impl MockLockedFile {
    fn new(file: FileHandle) -> Result<Self> {
        file.lock()?;
        Ok(Self(file))
    }
}

impl Drop for MockLockedFile {
    fn drop(&mut self) {
        self.0.unlock();
    }
}

impl LockedFile for MockLockedFile {
    fn unlock(self) -> Result<()> {
        // Unlock on drop.
        Ok(())
    }
}

struct MockPositionalFile(FileHandle);

impl PositionalFile for MockPositionalFile {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if offset > usize::MAX as u64 {
            return Ok(0);
        }
        Ok(self.0.read(buf, offset as usize))
    }
}

struct MockSequentialFile {
    file: FileHandle,
    offset: usize,
}

impl MockSequentialFile {
    fn new(file: FileHandle) -> Self {
        Self { file, offset: 0 }
    }
}

impl SequentialFile for MockSequentialFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let len = self.file.read(buf, self.offset);
        self.offset += len;
        Ok(len)
    }

    fn offset(&self) -> u64 {
        self.offset as u64
    }
}

impl SequentialFileWriter for MockSequentialFile {
    fn sync(&mut self) -> Result<()> {
        Ok(())
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.file.write(buf, self.offset);
        self.offset += buf.len();
        Ok(buf.len())
    }

    fn offset(&self) -> u64 {
        self.offset as u64
    }
}

#[derive(Clone)]
enum Handle {
    Dir(DirHandle),
    File(FileHandle),
}

#[derive(Clone, Default)]
struct DirHandle(Arc<Mutex<HashMap<String, Handle>>>);

impl DirHandle {
    fn list(&self) -> Vec<String> {
        let inner = self.0.lock().unwrap();
        inner.keys().cloned().collect()
    }

    fn open_dir(&self, name: &str) -> Result<DirHandle> {
        let inner = self.0.lock().unwrap();
        match inner.get(name).cloned() {
            Some(Handle::Dir(dir)) => Ok(dir),
            Some(_) => Err(ErrorKind::NotADirectory.into()),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    fn create_dir(&self, name: &str) -> Result<DirHandle> {
        let mut inner = self.0.lock().unwrap();
        match inner.get(name).cloned() {
            Some(Handle::Dir(dir)) => Ok(dir),
            Some(_) => Err(ErrorKind::NotADirectory.into()),
            None => {
                let dir = DirHandle::default();
                inner.insert(name.into(), Handle::Dir(dir.clone()));
                Ok(dir)
            }
        }
    }

    fn delete_dir(&self, name: &str) -> Result<()> {
        let mut inner = self.0.lock().unwrap();
        match inner.get(name) {
            Some(Handle::Dir(_)) => {
                inner.remove(name);
                Ok(())
            }
            Some(_) => Err(ErrorKind::NotADirectory.into()),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    fn open_file(&self, name: &str) -> Result<FileHandle> {
        let inner = self.0.lock().unwrap();
        match inner.get(name).cloned() {
            Some(Handle::File(file)) => Ok(file),
            Some(_) => Err(ErrorKind::IsADirectory.into()),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    fn create_file(&self, name: &str) -> Result<FileHandle> {
        let mut inner = self.0.lock().unwrap();
        match inner.get(name).cloned() {
            Some(Handle::File(file)) => {
                file.clear();
                Ok(file)
            }
            Some(_) => Err(ErrorKind::IsADirectory.into()),
            None => {
                let file = FileHandle::default();
                inner.insert(name.into(), Handle::File(file.clone()));
                Ok(file)
            }
        }
    }

    fn delete_file(&self, name: &str) -> Result<()> {
        let mut inner = self.0.lock().unwrap();
        match inner.get(name) {
            Some(Handle::File(_)) => {
                inner.remove(name);
                Ok(())
            }
            Some(_) => Err(ErrorKind::IsADirectory.into()),
            None => Err(ErrorKind::NotFound.into()),
        }
    }

    fn rename_file(&self, from: &str, to: &str) -> Result<()> {
        let mut inner = self.0.lock().unwrap();
        match inner.get(from).cloned() {
            Some(Handle::File(file)) => {
                inner.remove(from);
                inner.insert(to.into(), Handle::File(file));
                Ok(())
            }
            Some(_) => Err(ErrorKind::IsADirectory.into()),
            None => Err(ErrorKind::NotFound.into()),
        }
    }
}

#[derive(Default)]
struct FileInner {
    data: Vec<u8>,
    is_locked: bool,
}

#[derive(Clone, Default)]
struct FileHandle(Arc<Mutex<FileInner>>);

impl FileHandle {
    fn data(&self) -> Vec<u8> {
        let inner = self.0.lock().unwrap();
        inner.data.clone()
    }

    fn read(&self, buf: &mut [u8], offset: usize) -> usize {
        let inner = self.0.lock().unwrap();
        if offset >= inner.data.len() {
            return 0;
        }
        let src = &inner.data[offset..];
        let len = buf.len().min(src.len());
        buf[..len].copy_from_slice(&src[..len]);
        len
    }

    fn write(&self, buf: &[u8], offset: usize) {
        let mut inner = self.0.lock().unwrap();
        inner.data.resize(offset, 0);
        inner.data.extend_from_slice(buf);
    }

    fn clear(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.data.clear();
    }

    fn lock(&self) -> Result<()> {
        let mut inner = self.0.lock().unwrap();
        if inner.is_locked {
            return Err(ErrorKind::WouldBlock.into());
        }
        inner.is_locked = true;
        Ok(())
    }

    fn unlock(&self) {
        let mut inner = self.0.lock().unwrap();
        assert!(inner.is_locked);
        inner.is_locked = false;
    }
}
