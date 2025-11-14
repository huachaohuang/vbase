use std::collections::BTreeSet;
use std::io::ErrorKind;

use vbase_env::{Dir, LockedFile};
use vbase_file::{JournalFile, JournalFileWriter};

use crate::error::{Error, Result};

#[derive(Default)]
pub(crate) struct FileSet {
    pub(crate) journals: BTreeSet<u64>,
    pub(crate) manifests: BTreeSet<u64>,
    pub(crate) collections: BTreeSet<u64>,
}

pub(crate) struct RootDir {
    dir: Box<dyn Dir>,
    name: String,
    lock: Box<dyn LockedFile>,
}

impl RootDir {
    const LOCK: &str = "LOCK";
    const TEMP: &str = "TEMP";
    const CURRENT: &str = "CURRENT";

    pub(crate) fn lock(dir: Box<dyn Dir>, name: String) -> Result<Self> {
        let lock = dir
            .lock_file(Self::LOCK)
            .map_err(|e| Error::io(e, format!("lock {}", Self::LOCK)))?;
        Ok(Self { dir, name, lock })
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn list(&self) -> Result<FileSet> {
        let mut list = FileSet::default();
        let names = self
            .dir
            .list()
            .map_err(|e| Error::io(e, format!("list {}", self.name)))?;
        for name in names.iter().filter_map(|name| Name::parse(name)) {
            let _ = match name {
                Name::Journal(id) => list.journals.insert(id),
                Name::Manifest(id) => list.manifests.insert(id),
                Name::Collection(id) => list.collections.insert(id),
            };
        }
        Ok(list)
    }

    pub(crate) fn open_journal(&self, id: u64) -> Result<JournalFile> {
        let name = Name::journal(id);
        let file = self
            .dir
            .open_sequential_file(&name)
            .map_err(|e| Error::io(e, format!("open {name}")))?;
        Ok(JournalFile::open(file, name))
    }

    pub(crate) fn create_journal(&self, id: u64) -> Result<JournalFileWriter> {
        let name = Name::journal(id);
        let file = self
            .dir
            .create_sequential_file(&name)
            .map_err(|e| Error::io(e, format!("create {name}")))?;
        Ok(JournalFileWriter::open(file, name))
    }

    pub(crate) fn delete_journal(&self, id: u64) -> Result<()> {
        let name = Name::journal(id);
        self.dir
            .delete_file(&name)
            .map_err(|e| Error::io(e, format!("delete {name}")))
    }

    pub(crate) fn read_current(&self) -> Result<Option<u64>> {
        match self.dir.read_file(Self::CURRENT) {
            Ok(data) => {
                let name = String::from_utf8_lossy(&data);
                match Name::parse(&name) {
                    Some(Name::Manifest(id)) => Ok(Some(id)),
                    _ => Err(Error::corrupted(
                        Self::CURRENT,
                        format!("invalid manifest name {name}"),
                    )),
                }
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::io(e, format!("read {}", Self::CURRENT))),
        }
    }

    pub(crate) fn switch_current(&self, id: u64) -> Result<()> {
        let name = Name::manifest(id);
        self.dir
            .write_file(Self::TEMP, name.as_bytes())
            .map_err(|e| Error::io(e, format!("write {}", Self::TEMP)))?;
        self.dir
            .rename_file(Self::TEMP, Self::CURRENT)
            .map_err(|e| Error::io(e, format!("rename {} to {}", Self::TEMP, Self::CURRENT)))
    }

    pub(crate) fn open_manifest(&self, id: u64) -> Result<JournalFile> {
        let name = Name::manifest(id);
        let file = self
            .dir
            .open_sequential_file(&name)
            .map_err(|e| Error::io(e, format!("open {name}")))?;
        Ok(JournalFile::open(file, name))
    }

    pub(crate) fn create_manifest(&self, id: u64) -> Result<JournalFileWriter> {
        let name = Name::manifest(id);
        let file = self
            .dir
            .create_sequential_file(&name)
            .map_err(|e| Error::io(e, format!("create {name}")))?;
        Ok(JournalFileWriter::open(file, name))
    }

    pub(crate) fn delete_manifest(&self, id: u64) -> Result<()> {
        let name = Name::manifest(id);
        self.dir
            .delete_file(&name)
            .map_err(|e| Error::io(e, format!("delete {name}")))
    }

    pub(crate) fn open_collection(&self, id: u64) -> Result<Box<dyn Dir>> {
        let name = Name::collection(id);
        self.dir
            .open_dir(&name)
            .map_err(|e| Error::io(e, format!("open {name}")))
    }

    pub(crate) fn create_collection(&self, id: u64) -> Result<Box<dyn Dir>> {
        let name = Name::collection(id);
        self.dir
            .create_dir(&name)
            .map_err(|e| Error::io(e, format!("create {name}")))
    }

    pub(crate) fn delete_collection(&self, id: u64) -> Result<()> {
        let name = Name::collection(id);
        self.dir
            .delete_dir(&name)
            .map_err(|e| Error::io(e, format!("delete {name}")))
    }
}

enum Name {
    Journal(u64),
    Manifest(u64),
    Collection(u64),
}

impl Name {
    fn parse(name: &str) -> Option<Self> {
        if let Some(suffix) = name.strip_prefix("journal-") {
            suffix.parse().ok().map(Self::Journal)
        } else if let Some(suffix) = name.strip_prefix("manifest-") {
            suffix.parse().ok().map(Self::Manifest)
        } else if let Some(suffix) = name.strip_prefix("collection-") {
            suffix.parse().ok().map(Self::Collection)
        } else {
            None
        }
    }

    fn journal(id: u64) -> String {
        format!("journal-{id}")
    }

    fn manifest(id: u64) -> String {
        format!("manifest-{id}")
    }

    fn collection(id: u64) -> String {
        format!("collection-{id}")
    }
}
