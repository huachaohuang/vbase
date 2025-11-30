use std::fs;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result;
use std::io::Write;
use std::path::PathBuf;

use crate::Dir;
use crate::Env;
use crate::LockedFile;
use crate::PositionalFile;
use crate::SequentialFile;
use crate::SequentialFileWriter;

/// An implementation of [`Env`] based on the local file system.
pub struct LocalEnv;

impl Env for LocalEnv {
    fn name(&self) -> &str {
        "LocalEnv"
    }

    fn open_dir(&self, name: &str) -> Result<Box<dyn Dir>> {
        let dir = LocalDir::open(name)?;
        Ok(Box::new(dir))
    }

    fn create_dir(&self, name: &str) -> Result<Box<dyn Dir>> {
        let dir = LocalDir::create(name)?;
        Ok(Box::new(dir))
    }

    fn delete_dir(&self, name: &str) -> Result<()> {
        fs::remove_dir_all(name)
    }
}

/// An implementation of [`Dir`] based on the local file system.
pub struct LocalDir {
    path: PathBuf,
}

impl LocalDir {
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Self> {
        let path = path.into();
        if fs::metadata(&path)?.is_dir() {
            Ok(Self { path })
        } else {
            Err(ErrorKind::NotADirectory.into())
        }
    }

    pub fn create<P: Into<PathBuf>>(path: P) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        Ok(Self { path })
    }
}

impl Dir for LocalDir {
    fn list(&self) -> Result<Vec<String>> {
        let dir = fs::read_dir(&self.path)?;
        dir.map(|res| {
            res.and_then(|ent| {
                ent.file_name()
                    .into_string()
                    .map_err(|name| Error::new(ErrorKind::InvalidFilename, format!("{name:?}")))
            })
        })
        .collect()
    }

    fn open_dir(&self, name: &str) -> Result<Box<dyn Dir>> {
        let dir = Self::open(self.path.join(name))?;
        Ok(Box::new(dir))
    }

    fn create_dir(&self, name: &str) -> Result<Box<dyn Dir>> {
        let dir = Self::create(self.path.join(name))?;
        Ok(Box::new(dir))
    }

    fn delete_dir(&self, name: &str) -> Result<()> {
        fs::remove_dir_all(self.path.join(name))
    }

    fn lock_file(&self, name: &str) -> Result<Box<dyn LockedFile>> {
        let path = self.path.join(name);
        let file = fs::File::create(path)?;
        file.try_lock()?;
        Ok(Box::new(LocalFile(file)))
    }

    fn read_file(&self, name: &str) -> Result<Vec<u8>> {
        fs::read(self.path.join(name))
    }

    fn write_file(&self, name: &str, data: &[u8]) -> Result<()> {
        let path = self.path.join(name);
        let mut file = fs::File::create(path)?;
        file.write_all(data)?;
        file.sync_all()?;
        Ok(())
    }

    fn delete_file(&self, name: &str) -> Result<()> {
        fs::remove_file(self.path.join(name))
    }

    fn rename_file(&self, from: &str, to: &str) -> Result<()> {
        fs::rename(self.path.join(from), self.path.join(to))
    }

    fn open_positional_file(&self, name: &str) -> Result<Box<dyn PositionalFile>> {
        let path = self.path.join(name);
        let file = fs::File::open(path)?;
        Ok(Box::new(LocalFile(file)))
    }

    fn open_sequential_file(&self, name: &str) -> Result<Box<dyn SequentialFile>> {
        let path = self.path.join(name);
        let file = fs::File::open(path)?;
        Ok(Box::new(LocalSequentialFile::new(file)))
    }

    fn create_sequential_file(&self, name: &str) -> Result<Box<dyn SequentialFileWriter>> {
        let path = self.path.join(name);
        let file = fs::File::create(path)?;
        Ok(Box::new(LocalSequentialFile::new(file)))
    }
}

struct LocalFile(fs::File);

impl LockedFile for LocalFile {
    fn unlock(self) -> Result<()> {
        self.0.unlock()
    }
}

impl PositionalFile for LocalFile {
    #[cfg(unix)]
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::os::unix::fs::FileExt;
        self.0.read_at(buf, offset)
    }

    #[cfg(windows)]
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::os::windows::fs::FileExt;
        self.0.seek_read(buf, offset)
    }
}

struct LocalSequentialFile {
    file: fs::File,
    offset: u64,
}

impl LocalSequentialFile {
    fn new(file: fs::File) -> Self {
        Self { file, offset: 0 }
    }
}

impl SequentialFile for LocalSequentialFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.file.read(buf).inspect(|&n| self.offset += n as u64)
    }

    fn offset(&self) -> u64 {
        self.offset
    }
}

impl SequentialFileWriter for LocalSequentialFile {
    fn sync(&mut self) -> Result<()> {
        self.file.sync_data()
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.file.write(buf).inspect(|&n| self.offset += n as u64)
    }

    fn offset(&self) -> u64 {
        self.offset
    }
}
