use std::collections::BTreeSet;

use vbase_env::Dir;
use vbase_env::LockedFile;
use vbase_file::Result;
use vbase_file::error::Context;
pub use vbase_file::journal::File as JournalFile;
pub use vbase_file::journal::FileWriter as JournalFileWriter;

#[derive(Default)]
pub(crate) struct FileSet {
    pub(crate) engines: BTreeSet<String>,
    pub(crate) journals: BTreeSet<u64>,
}

pub(crate) struct RootDir {
    dir: Box<dyn Dir>,
    path: String,
    #[expect(unused)]
    lock: Option<Box<dyn LockedFile>>,
}

impl RootDir {
    const LOCK: &str = "LOCK";

    pub(crate) fn lock(dir: Box<dyn Dir>, path: String) -> Result<Self> {
        let lock = dir
            .lock_file(Self::LOCK)
            .context(|| format!("lock {}", Self::LOCK))?;
        Ok(Self {
            dir,
            path,
            lock: Some(lock),
        })
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn list(&self) -> Result<FileSet> {
        let mut fset = FileSet::default();
        let list = self.dir.list().context(|| format!("list {}", self.path))?;
        for name in list.iter().filter_map(|name| Name::parse(name)) {
            let _ = match name {
                Name::Engine(name) => fset.engines.insert(name),
                Name::Journal(id) => fset.journals.insert(id),
            };
        }
        Ok(fset)
    }

    pub(crate) fn open_engine(&self, name: &str) -> Result<Box<dyn Dir>> {
        let name = Name::engine(name);
        self.dir.open_dir(&name).context(|| format!("open {name}"))
    }

    pub(crate) fn create_engine(&self, name: &str) -> Result<Box<dyn Dir>> {
        let name = Name::engine(name);
        self.dir
            .create_dir(&name)
            .context(|| format!("create {name}"))
    }

    pub(crate) fn delete_engine(&self, name: &str) -> Result<()> {
        let name = Name::engine(name);
        self.dir
            .delete_dir(&name)
            .context(|| format!("delete {name}"))
    }

    pub(crate) fn open_journal(&self, id: u64) -> Result<JournalFile> {
        let name = Name::journal(id);
        let file = self
            .dir
            .open_sequential_file(&name)
            .context(|| format!("open {name}"))?;
        Ok(JournalFile::open(file, name))
    }

    pub(crate) fn create_journal(&self, id: u64) -> Result<JournalFileWriter> {
        let name = Name::journal(id);
        let file = self
            .dir
            .create_sequential_file(&name)
            .context(|| format!("create {name}"))?;
        Ok(JournalFileWriter::open(file, name))
    }

    pub(crate) fn delete_journal(&self, id: u64) -> Result<()> {
        let name = Name::journal(id);
        self.dir
            .delete_file(&name)
            .context(|| format!("delete {name}"))?;
        Ok(())
    }
}

enum Name {
    Engine(String),
    Journal(u64),
}

impl Name {
    fn parse(name: &str) -> Option<Self> {
        if let Some(suffix) = name.strip_prefix("engine.") {
            suffix.parse().ok().map(Self::Engine)
        } else if let Some(suffix) = name.strip_prefix("journal-") {
            suffix.parse().ok().map(Self::Journal)
        } else {
            None
        }
    }

    fn engine(name: &str) -> String {
        format!("engine.{name}")
    }

    fn journal(id: u64) -> String {
        format!("journal-{id}")
    }
}
