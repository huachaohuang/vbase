use std::collections::BTreeSet;
use std::io::ErrorKind;

use vbase_engine::env::boxed::Dir;
use vbase_engine::env::boxed::SequentialFile;
use vbase_engine::env::boxed::SequentialFileWriter;

use crate::Result;
use crate::error::Corrupted;

#[derive(Default)]
pub(crate) struct FileSet {
    pub(crate) manifests: BTreeSet<u64>,
}

pub(crate) struct RootDir {
    dir: Dir,
}

impl RootDir {
    const TEMP: &str = "TEMP";
    const CURRENT: &str = "CURRENT";

    pub(crate) fn new(dir: Dir) -> Self {
        Self { dir }
    }

    pub(crate) fn list(&self) -> Result<FileSet> {
        let mut list = FileSet::default();
        let names = self.dir.list()?;
        for name in names.iter().filter_map(|name| Name::parse(name)) {
            let _ = match name {
                Name::Manifest(id) => list.manifests.insert(id),
            };
        }
        Ok(list)
    }

    pub(crate) fn read_current(&self) -> Result<Option<u64>> {
        match self.dir.read_file(Self::CURRENT) {
            Ok(data) => {
                let name = String::from_utf8_lossy(&data);
                match Name::parse(&name) {
                    Some(Name::Manifest(id)) => Ok(Some(id)),
                    _ => Self::CURRENT.corrupted(format!("invalid manifest name {name}")),
                }
            }
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn switch_current(&self, id: u64) -> Result<()> {
        let name = Name::manifest(id);
        self.dir.write_file(Self::TEMP, name.as_bytes())?;
        self.dir.rename_file(Self::TEMP, Self::CURRENT)?;
        Ok(())
    }

    pub(crate) fn open_manifest(&self, id: u64) -> Result<SequentialFile> {
        let name = Name::manifest(id);
        self.dir.open_sequential_file(&name).map_err(Into::into)
    }

    pub(crate) fn create_manifest(&self, id: u64) -> Result<SequentialFileWriter> {
        let name = Name::manifest(id);
        self.dir.create_sequential_file(&name).map_err(Into::into)
    }

    pub(crate) fn delete_manifest(&self, id: u64) -> Result<()> {
        let name = Name::manifest(id);
        self.dir.delete_file(&name).map_err(Into::into)
    }
}

enum Name {
    Manifest(u64),
}

impl Name {
    fn parse(name: &str) -> Option<Self> {
        if let Some(suffix) = name.strip_prefix("manifest-") {
            suffix.parse().ok().map(Self::Manifest)
        } else {
            None
        }
    }

    fn manifest(id: u64) -> String {
        format!("manifest-{id}")
    }
}
