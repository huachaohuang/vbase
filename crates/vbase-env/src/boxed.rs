use std::fmt;
use std::io::Error;
use std::io::Result;
use std::sync::Arc;

/// A wrapper for [`crate::Env`] objects.
#[derive(Clone)]
pub struct Env(Arc<dyn crate::Env>);

impl Env {
    /// Creates a wrapper for `env`.
    pub fn new<E: crate::Env + 'static>(env: E) -> Self {
        Self(Arc::new(env))
    }

    /// Creates a wrapper for [`crate::TestEnv`].
    #[cfg(feature = "test")]
    pub fn test() -> Result<Self> {
        let env = crate::TestEnv::new().context(|| "create TestEnv")?;
        Ok(Self::new(env))
    }

    /// See [`crate::Env::name`].
    pub fn name(&self) -> &str {
        self.0.name()
    }

    /// See [`crate::Env::open_dir`].
    pub fn open_dir(&self, name: &str) -> Result<Dir> {
        let dir = self.0.open_dir(name).context(|| format!("open {name}"))?;
        Ok(Dir::new(dir, "/"))
    }

    /// See [`crate::Env::create_dir`].
    pub fn create_dir(&self, name: &str) -> Result<Dir> {
        let dir = self
            .0
            .create_dir(name)
            .context(|| format!("create {name}"))?;
        Ok(Dir::new(dir, "/"))
    }

    /// See [`crate::Env::delete_dir`].
    pub fn delete_dir(&self, name: &str) -> Result<()> {
        self.0.delete_dir(name).context(|| format!("delete {name}"))
    }
}

impl Default for Env {
    /// Creates a wrapper for [`crate::LocalEnv`].
    fn default() -> Self {
        Self::new(crate::LocalEnv)
    }
}

impl fmt::Debug for Env {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A wrapper for [`crate::Dir`] objects.
pub struct Dir {
    dir: Box<dyn crate::Dir>,
    path: String,
}

impl Dir {
    fn new(dir: Box<dyn crate::Dir>, path: impl Into<String>) -> Self {
        Self {
            dir,
            path: path.into(),
        }
    }

    /// Creates a wrapper for [`crate::TestDir`].
    #[cfg(feature = "test")]
    pub fn test() -> Result<Self> {
        let dir = crate::TestDir::new().context(|| "create TestDir")?;
        Ok(Self::new(Box::new(dir), "/"))
    }

    fn join(&self, name: &str) -> String {
        if self.path.ends_with('/') {
            format!("{}{}", self.path, name)
        } else {
            format!("{}/{}", self.path, name)
        }
    }

    /// Returns the path relative to the environment root.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// See [`crate::Dir::list`].
    pub fn list(&self) -> Result<Vec<String>> {
        self.dir.list().context(|| format!("list {}", self.path))
    }

    /// See [`crate::Dir::open_dir`].
    pub fn open_dir(&self, name: &str) -> Result<Dir> {
        let path = self.join(name);
        self.dir
            .open_dir(name)
            .context(|| format!("open {path}"))
            .map(|dir| Dir::new(dir, path))
    }

    /// See [`crate::Dir::create_dir`].
    pub fn create_dir(&self, name: &str) -> Result<Dir> {
        let path = self.join(name);
        self.dir
            .create_dir(name)
            .context(|| format!("create {path}"))
            .map(|dir| Dir::new(dir, path))
    }

    /// See [`crate::Dir::delete_dir`].
    pub fn delete_dir(&self, name: &str) -> Result<()> {
        let path = self.join(name);
        self.dir
            .delete_dir(name)
            .context(|| format!("delete {path}"))
    }

    /// See [`crate::Dir::lock_file`].
    pub fn lock_file(&self, name: &str) -> Result<LockedFile> {
        self.dir
            .lock_file(name)
            .context(|| format!("lock {}", self.join(name)))
            .map(|file| LockedFile { file })
    }

    /// See [`crate::Dir::read_file`].
    pub fn read_file(&self, name: &str) -> Result<Vec<u8>> {
        self.dir
            .read_file(name)
            .context(|| format!("read {}", self.join(name)))
    }

    /// See [`crate::Dir::write_file`].
    pub fn write_file(&self, name: &str, data: &[u8]) -> Result<()> {
        self.dir
            .write_file(name, data)
            .context(|| format!("write {}", self.join(name)))
    }

    /// See [`crate::Dir::delete_file`].
    pub fn delete_file(&self, name: &str) -> Result<()> {
        self.dir
            .delete_file(name)
            .context(|| format!("delete {}", self.join(name)))
    }

    /// See [`crate::Dir::rename_file`].
    pub fn rename_file(&self, from: &str, to: &str) -> Result<()> {
        self.dir
            .rename_file(from, to)
            .context(|| format!("rename {} to {}", self.join(from), self.join(to)))
    }

    /// See [`crate::Dir::open_positional_file`].
    pub fn open_positional_file(&self, name: &str) -> Result<PositionalFile> {
        let path = self.join(name);
        self.dir
            .open_positional_file(name)
            .context(|| format!("open {path}"))
            .map(|file| PositionalFile { file, path })
    }

    /// See [`crate::Dir::open_sequential_file`].
    pub fn open_sequential_file(&self, name: &str) -> Result<SequentialFile> {
        let path = self.join(name);
        self.dir
            .open_sequential_file(name)
            .context(|| format!("open {path}"))
            .map(|file| SequentialFile { file, path })
    }

    /// See [`crate::Dir::create_sequential_file`].
    pub fn create_sequential_file(&self, name: &str) -> Result<SequentialFileWriter> {
        let path = self.join(name);
        self.dir
            .create_sequential_file(name)
            .context(|| format!("create {path}"))
            .map(|file| SequentialFileWriter { file, path })
    }
}

impl fmt::Debug for Dir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.path)
    }
}

/// A wrapper for [`crate::LockedFile`] objects.
pub struct LockedFile {
    #[allow(dead_code)]
    file: Box<dyn crate::LockedFile>,
}

impl crate::LockedFile for LockedFile {}

/// A wrapper for [`crate::PositionalFile`] objects.
pub struct PositionalFile {
    file: Box<dyn crate::PositionalFile>,
    path: String,
}

impl PositionalFile {
    /// Returns the path relative to the environment root.
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl crate::PositionalFile for PositionalFile {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.file
            .read(buf, offset)
            .context(|| format!("read {} at offset {}", self.path, offset))
    }
}

/// A wrapper for [`crate::SequentialFile`] objects.
pub struct SequentialFile {
    file: Box<dyn crate::SequentialFile>,
    path: String,
}

impl SequentialFile {
    /// Returns the path relative to the environment root.
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl crate::SequentialFile for SequentialFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.file
            .read(buf)
            .context(|| format!("read {} at offset {}", self.path, self.offset()))
    }

    fn offset(&self) -> u64 {
        self.file.offset()
    }
}

/// A wrapper for [`crate::SequentialFileWriter`] objects.
pub struct SequentialFileWriter {
    file: Box<dyn crate::SequentialFileWriter>,
    path: String,
}

impl SequentialFileWriter {
    /// Returns the path relative to the environment root.
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl crate::SequentialFileWriter for SequentialFileWriter {
    fn sync(&mut self) -> Result<()> {
        self.file.sync().context(|| format!("sync {}", self.path))
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.file
            .write(buf)
            .context(|| format!("write {} at offset {}", self.path, self.offset()))
    }

    fn offset(&self) -> u64 {
        self.file.offset()
    }
}

/// An extension to add context to [`Error`].
trait Context<T> {
    fn context<F, C>(self, context: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: Into<String>;
}

impl<T> Context<T> for Result<T> {
    fn context<F, C>(self, context: F) -> Result<T>
    where
        F: FnOnce() -> C,
        C: Into<String>,
    {
        self.map_err(|e| {
            let context = context().into();
            Error::new(e.kind(), format!("{context}: {e}"))
        })
    }
}
