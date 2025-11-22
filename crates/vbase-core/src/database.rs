use std::collections::HashMap;
use std::fmt;
use std::io::ErrorKind;

use log::info;
use vbase_file::error::Context;
use vbase_file::error::Corrupted;
use vbase_util::sync::Arc;
use vbase_util::sync::Mutex;

use crate::Error;
use crate::Result;
use crate::engine::Engine;
use crate::engine::Handle;
use crate::file::RootDir;
use crate::journal::JournalWriter;
use crate::manifest::Desc;
use crate::manifest::EngineDesc;
use crate::options::Builder;
use crate::options::Options;
use crate::options::WriteOptions;
use crate::pipeline::WriteCommitter;
use crate::pipeline::WriteSubmitter;
use crate::pipeline::create_pipeline;

pub struct Database {
    root: RootDir,
    options: Options,
    engines: Engines,

    /// Writes are processed in a pipeline to improve throughput.
    ///
    /// The write flow is split into two stages: submit and commit.
    /// In the submit stage, locking is required to determine the order of
    /// writes. In the commit stage, writers can update engines and commits
    /// their writes without locking.
    ///
    /// The write flow should be as follows:
    ///
    /// 1. Lock the state
    /// 2. Write to the journal
    /// 3. Submit the write to the submitter
    /// 4. Unlock the state
    /// 5. Update the engines
    /// 6. Commit the write to the committer
    state: Mutex<State>,
    committer: WriteCommitter,
}

impl Database {
    pub(crate) fn open(path: &str, mut builder: Builder) -> Result<Self> {
        let options = builder.options;
        info!("open {path} with {options:#?}");

        // Open or create `path`.
        let dir = match options.env.open_dir(path) {
            Ok(dir) => dir,
            Err(e) if e.kind() != ErrorKind::NotFound => {
                return e.context(|| format!("open {path}"))?;
            }
            Err(_) if builder.error_if_not_exist => {
                return Err(Error::NotExist(path.into()));
            }
            Err(_) => options
                .env
                .create_dir(path)
                .context(|| format!("create {path}"))?,
        };
        let root = RootDir::lock(dir, path.into())?;

        // Read the manifest file.
        let mut desc = match root.read_manifest()? {
            Some(_) if builder.error_if_exists => {
                return Err(Error::Exists(format!("manifest at {path}")));
            }
            Some(desc) => desc,
            None if builder.error_if_not_exist => {
                return Err(Error::NotExist(format!("manifest at {path}")));
            }
            None => Desc::default(),
        };

        // Clean up uncommitted engines.
        let list = root.list()?;
        for id in list.engines {
            if !desc.engines.iter().any(|e| e.id == id) {
                root.delete_engine(id)?;
            }
        }

        // Validate engines in the builder.
        for engine in &desc.engines {
            if !builder.engines.contains_key(&engine.name) {
                return Err(Error::InvalidArgument(format!(
                    "engine {} exists but not registered",
                    engine.name,
                )));
            }
        }

        // Open or create engines in the builder.
        let mut engines = HashMap::new();
        for (name, open) in builder.engines.drain() {
            let (id, handle) = match desc.engines.iter().find(|e| e.name == name) {
                Some(engine) => {
                    info!("open {engine:?}");
                    let dir = root.open_engine(engine.id)?;
                    let handle = open(engine.id, dir)?;
                    (engine.id, handle)
                }
                None => {
                    let id = desc.last_id + 1;
                    let engine = EngineDesc {
                        id,
                        name: name.clone(),
                    };
                    info!("create {engine:?}");
                    desc.last_id = id;
                    desc.engines.push(engine);
                    let dir = root.create_engine(id)?;
                    let handle = open(id, dir)?;
                    (id, handle)
                }
            };
            engines.insert(id, handle);
        }

        // Update the manifest to commit created engines.
        root.update_manifest(&desc)?;

        // Recover the previous state.
        let engines = Engines(engines);
        let mut recover = Recover::new(root, engines)?;
        recover.recover()?;
        let Recover {
            root,
            engines,
            last_lsn,
        } = recover;
        let journal = root.create_journal(last_lsn + 1)?;
        let (submitter, committer) = create_pipeline(last_lsn);
        let state = State { journal, submitter };

        Ok(Self {
            root,
            options,
            engines,
            state: Mutex::new(state),
            committer,
        })
    }

    pub fn write(&self, batch: &WriteBatch, options: &WriteOptions) -> Result<()> {
        todo!()
    }

    pub fn collection<E: Engine>(&self, name: &str) -> Result<E::Collection> {
        let Some(engine) = self.engines.find(E::NAME) else {
            return Err(Error::InvalidArgument(format!(
                "engine {} does not exist",
                E::NAME
            )));
        };

        let collection = engine.collection(name)?;
        E::collection(collection)
    }

    pub fn create_collection<E: Engine>(&self, name: &str) -> Result<E::Collection> {
        let Some(engine) = self.engines.find(E::NAME) else {
            return Err(Error::InvalidArgument(format!(
                "engine {} does not exist",
                E::NAME
            )));
        };

        info!("create collection {} in engine {}", name, E::NAME);
        let collection = engine.create_collection(name)?;
        E::collection(collection)
    }

    pub fn delete_collection<E: Engine>(&self, name: &str) -> Result<()> {
        let Some(engine) = self.engines.find(E::NAME) else {
            return Err(Error::InvalidArgument(format!(
                "engine {} does not exist",
                E::NAME
            )));
        };

        info!("delete collection {} from engine {}", name, E::NAME);
        engine.delete_collection(name)
    }
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Database")
            .field("path", &self.root.path())
            .field("options", &self.options)
            .finish()
    }
}

struct State {
    journal: JournalWriter,
    submitter: WriteSubmitter,
}

struct Engines(HashMap<u64, Arc<dyn Handle>>);

impl Engines {
    /// Finds an engine.
    fn find(&self, name: &str) -> Option<&Arc<dyn Handle>> {
        self.0.values().find(|h| h.name() == name)
    }

    /// Writes a batch to engines.
    fn write(&self, lsn: u64, batch: &[u8]) {
        for (id, batch) in WriteBatchIter(batch) {
            if let Some(engine) = self.0.get(&id) {
                engine.write(lsn, batch);
            }
        }
    }

    /// Recovers engines from a write batch.
    fn recover(&self, lsn: u64, batch: &[u8]) {
        for (id, batch) in WriteBatchIter(batch) {
            if let Some(engine) = self.0.get(&id) {
                if engine.last_lsn() >= lsn {
                    continue;
                }
                engine.write(lsn, batch);
            }
        }
    }

    /// Returns the minimum last LSN among all engines.
    fn min_last_lsn(&self) -> u64 {
        self.0.values().map(|db| db.last_lsn()).min().unwrap_or(0)
    }

    /// Returns the maximum last LSN among all engines.
    fn max_last_lsn(&self) -> u64 {
        self.0.values().map(|db| db.last_lsn()).max().unwrap_or(0)
    }
}

struct Recover {
    root: RootDir,
    engines: Engines,
    last_lsn: u64,
}

impl Recover {
    fn new(root: RootDir, engines: Engines) -> Result<Self> {
        Ok(Self {
            root,
            engines,
            last_lsn: 0,
        })
    }

    fn recover(&mut self) -> Result<()> {
        for id in self.journals_to_recover()? {
            info!("recover from journal {id}");
            let mut journal = self.root.open_journal(id)?;
            while let Some((lsn, batch)) = journal.read()? {
                self.engines.recover(lsn, batch);
                self.last_lsn = lsn;
            }
        }

        let max_lsn = self.engines.max_last_lsn();
        if self.last_lsn < max_lsn {
            return self.root.path().corrupted(format!(
                "the last LSN {} in journal files is less than \
                the maximum last LSN {} in engines, \
                which means that some journal files are missing or corrupted, \
                and we can not recover to a consistent state",
                self.last_lsn, max_lsn,
            ))?;
        }

        Ok(())
    }

    /// Returns the journal files that need to be recovered.
    fn journals_to_recover(&self) -> Result<Vec<u64>> {
        let list = self.root.list()?;
        let min_lsn = self.engines.min_last_lsn();
        let mut iter = list.journals.into_iter().peekable();
        let mut first = None;
        while let Some(id) = iter.next_if(|&id| id <= min_lsn) {
            first = Some(id);
        }
        Ok(first.into_iter().chain(iter).collect())
    }
}

/// A batch of updates.
#[derive(Clone, Default)]
pub struct WriteBatch {
    buf: Vec<u8>,
}

impl AsRef<[u8]> for WriteBatch {
    fn as_ref(&self) -> &[u8] {
        self.buf.as_ref()
    }
}

/// An iterator over a write batch.
struct WriteBatchIter<'a>(&'a [u8]);

impl<'a> Iterator for WriteBatchIter<'a> {
    type Item = (u64, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        todo!()
    }
}
