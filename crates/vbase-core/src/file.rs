use std::collections::BTreeSet;
use std::io::ErrorKind;

use vbase_env::boxed::Dir;
use vbase_env::boxed::LockedFile;

use crate::Error;
use crate::Result;
use crate::error::Corrupted;
use crate::journal::Journal;
use crate::journal::JournalWriter;
use crate::manifest::Desc;

#[derive(Default)]
pub(crate) struct FileSet {
    pub(crate) engines: BTreeSet<u64>,
    pub(crate) journals: BTreeSet<u64>,
}

pub(crate) struct RootDir {
    dir: Dir,
    #[allow(dead_code)]
    lock: LockedFile,
}

impl RootDir {
    const LOCK: &str = "LOCK";
    const TEMP: &str = "TEMP";
    const MANIFEST: &str = "MANIFEST";

    pub(crate) fn lock(dir: Dir) -> Result<Self> {
        let lock = match dir.lock_file(Self::LOCK) {
            Ok(x) => x,
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                return Err(Error::Locked(dir.path().into()));
            }
            Err(e) => return Err(e.into()),
        };
        Ok(Self { dir, lock })
    }

    pub(crate) fn path(&self) -> &str {
        self.dir.path()
    }

    pub(crate) fn list(&self) -> Result<FileSet> {
        let mut list = FileSet::default();
        let names = self.dir.list()?;
        for name in names.iter().filter_map(|name| Name::parse(name)) {
            let _ = match name {
                Name::Engine(id) => list.engines.insert(id),
                Name::Journal(id) => list.journals.insert(id),
            };
        }
        Ok(list)
    }

    pub(crate) fn open_engine(&self, id: u64) -> Result<Dir> {
        let name = Name::engine(id);
        self.dir.open_dir(&name).map_err(Into::into)
    }

    pub(crate) fn create_engine(&self, id: u64) -> Result<Dir> {
        let name = Name::engine(id);
        self.dir.create_dir(&name).map_err(Into::into)
    }

    pub(crate) fn delete_engine(&self, id: u64) -> Result<()> {
        let name = Name::engine(id);
        self.dir.delete_dir(&name).map_err(Into::into)
    }

    pub(crate) fn open_journal(&self, id: u64) -> Result<Journal> {
        let name = Name::journal(id);
        let file = self.dir.open_sequential_file(&name)?;
        Ok(Journal::new(file))
    }

    pub(crate) fn create_journal(&self, id: u64) -> Result<JournalWriter> {
        let name = Name::journal(id);
        let file = self.dir.create_sequential_file(&name)?;
        Ok(JournalWriter::new(file))
    }

    pub(crate) fn delete_journal(&self, id: u64) -> Result<()> {
        let name = Name::journal(id);
        self.dir.delete_file(&name).map_err(Into::into)
    }

    pub(crate) fn read_manifest(&self) -> Result<Option<Desc>> {
        match self.dir.read_file(Self::MANIFEST) {
            Ok(x) => Desc::decode_with_checksum(x.as_slice())
                .map(Some)
                .or_else(|e| Self::MANIFEST.corrupted(e)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn switch_manifest(&self, desc: &Desc) -> Result<()> {
        let data = desc.encode_with_checksum();
        self.dir.write_file(Self::TEMP, &data)?;
        self.dir.rename_file(Self::TEMP, Self::MANIFEST)?;
        Ok(())
    }
}

enum Name {
    Engine(u64),
    Journal(u64),
}

impl Name {
    fn parse(name: &str) -> Option<Self> {
        if let Some(suffix) = name.strip_prefix("engine-") {
            suffix.parse().ok().map(Self::Engine)
        } else if let Some(suffix) = name.strip_prefix("journal-") {
            suffix.parse().ok().map(Self::Journal)
        } else {
            None
        }
    }

    fn engine(id: u64) -> String {
        format!("engine-{id}")
    }

    fn journal(id: u64) -> String {
        format!("journal-{id}")
    }
}
