use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::io::ErrorKind;

use log::info;
use vbase_util::cell::UnsafeCell;
use vbase_util::sync::Arc;
use vbase_util::sync::Mutex;
use vbase_util::sync::MutexGuard;

use crate::Error;
use crate::Result;
use crate::engine::Bucket;
use crate::engine::Engine;
use crate::engine::internal::BucketHandle;
use crate::engine::internal::EngineHandle;
use crate::engine::internal::WriteBatch as _;
use crate::error::Corrupted;
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

/// The core database structure.
pub struct Core {
    root: RootDir,
    options: Options,
    engines: Engines,

    /// Writes are processed in a pipeline to improve throughput.
    ///
    /// The write flow is split into two stages: submit and commit.
    /// In the submit stage, a lock is held to determine the order of writes.
    /// In the commit stage, the lock is released, writers can process and
    /// commit their writes in parallel.
    ///
    /// The write flow should be as follows:
    ///
    /// 1. Lock the journal
    /// 2. Write to the journal
    /// 3. Submit the write to the submitter
    /// 4. Unlock the journal
    /// 5. Update the engines
    /// 6. Commit the write to the committer
    journal: Mutex<JournalWriter>,
    submitter: UnsafeCell<WriteSubmitter>,
    committer: WriteCommitter,
}

impl Core {
    pub fn open(path: &str, options: Options, mut builder: Builder) -> Result<Self> {
        options.validate()?;
        builder.validate()?;
        info!("open {path} with {options:#?}");

        // Open or create `path`.
        let dir = match options.env.open_dir(path) {
            Ok(dir) => dir,
            Err(e) if e.kind() != ErrorKind::NotFound => return Err(e.into()),
            Err(_) if builder.error_if_not_exist => {
                return Err(Error::NotExist(path.into()));
            }
            Err(_) => options.env.create_dir(path)?,
        };
        let root = RootDir::lock(dir)?;

        // Read the manifest file.
        let mut desc = match root.read_manifest()? {
            Some(_) if builder.error_if_exists => {
                return Err(Error::Exists(format!("manifest in {path}")));
            }
            Some(desc) => desc,
            None if builder.error_if_not_exist => {
                return Err(Error::NotExist(format!("manifest in {path}")));
            }
            None => Desc::default(),
        };

        // Clean up uncommitted engines.
        let list = root.list()?;
        for id in list.engines {
            if !desc.engines.iter().any(|e| e.id == id) {
                info!("delete uncommitted engine {id}");
                root.delete_engine(id)?;
            }
        }

        // Validate engines in the builder.
        for name in desc.engines.iter().map(|e| &e.name) {
            if !builder.engines.contains_key(name) {
                return Err(Error::InvalidArgument(format!(
                    "engine {name} exists but not registered",
                )));
            }
        }

        // Open or create engines in the builder.
        let mut engines = HashMap::new();
        for (name, open) in builder.engines.drain() {
            let (id, dir) = match desc.engines.iter().find(|e| e.name == name) {
                Some(engine) => {
                    info!("open engine {} id {}", engine.name, engine.id);
                    let id = engine.id;
                    let dir = root.open_engine(id)?;
                    (id, dir)
                }
                None => {
                    let id = desc.last_id + 1;
                    let engine = EngineDesc {
                        id,
                        name: name.clone(),
                    };
                    info!("create engine {} id {}", engine.name, engine.id);
                    desc.last_id = id;
                    desc.engines.push(engine);
                    let dir = root.create_engine(id)?;
                    (id, dir)
                }
            };
            let handle = open(id, dir)?;
            engines.insert(id, handle);
        }

        // Commit created engines to the manifest.
        root.switch_manifest(&desc)?;

        // Recover to the previous state.
        let mut recover = Recover::new(root, Engines(engines));
        recover.recover()?;
        let Recover {
            root,
            engines,
            last_lsn,
        } = recover;
        let journal = root.create_journal(last_lsn + 1)?;
        let (submitter, committer) = create_pipeline(last_lsn);

        Ok(Self {
            root,
            options,
            engines,
            journal: Mutex::new(journal),
            submitter: UnsafeCell::new(submitter),
            committer,
        })
    }

    pub fn write(&self, batch: &WriteBatch, options: &WriteOptions) -> Result<()> {
        /// A guard that protects the journal and the submitter.
        ///
        /// The submitter requires exclusive access, but we can not put it in
        /// the lock with the journal because we need to hold the write handle
        /// after releasing the lock.
        struct Guard<'a> {
            journal: MutexGuard<'a, JournalWriter>,
            submitter: &'a mut WriteSubmitter,
        }

        let (lsn, handle) = {
            let mut guard = Guard {
                journal: self.journal.lock().unwrap(),
                submitter: unsafe { self.submitter.as_mut() },
            };
            let lsn = guard.submitter.next_lsn();
            guard.journal.write(lsn, batch.as_ref())?;
            if options.sync {
                guard.journal.sync()?;
            }
            // TODO: handle journal rotation
            let handle = guard.submitter.submit(lsn);
            (lsn, handle)
        };

        self.engines.write(lsn, batch.as_ref());
        self.committer.commit(handle);
        Ok(())
    }

    pub fn bucket<E: Engine>(&self, name: &str) -> Result<E::Bucket> {
        let Some(engine) = self.engines.find(E::NAME) else {
            return Err(Error::InvalidArgument(format!(
                "engine {} is not registered",
                E::NAME
            )));
        };

        let handle = engine.bucket(name)?;
        open_bucket::<E, E::Bucket>(handle)
    }

    pub fn create_bucket<E: Engine>(&self, name: &str) -> Result<E::Bucket> {
        let Some(engine) = self.engines.find(E::NAME) else {
            return Err(Error::InvalidArgument(format!(
                "engine {} is not registered",
                E::NAME
            )));
        };

        info!("create bucket {name} in engine {}", E::NAME);
        let handle = engine.create_bucket(name)?;
        open_bucket::<E, E::Bucket>(handle)
    }

    pub fn delete_bucket<E: Engine>(&self, name: &str) -> Result<()> {
        let Some(engine) = self.engines.find(E::NAME) else {
            return Err(Error::InvalidArgument(format!(
                "engine {} is not registered",
                E::NAME
            )));
        };

        info!("delete bucket {name} from engine {}", E::NAME);
        engine.delete_bucket(name)
    }
}

impl fmt::Debug for Core {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Core")
            .field("path", &self.root.path())
            .field("options", &self.options)
            .finish()
    }
}

fn open_bucket<E, B>(handle: Arc<dyn BucketHandle>) -> Result<B>
where
    E: Engine,
    B: Bucket,
{
    let handle = handle as Arc<dyn Any + Send + Sync>;
    let handle = handle.downcast::<B::Handle>().map_err(|_| {
        Error::InvalidArgument(format!("invalid bucket handle for engine {}", E::NAME))
    })?;
    Ok(B::open(handle))
}

struct Engines(HashMap<u64, Box<dyn EngineHandle>>);

impl Engines {
    /// Finds an engine.
    fn find(&self, name: &str) -> Option<&dyn EngineHandle> {
        self.0
            .values()
            .find_map(|h| (h.name() == name).then_some(h.as_ref()))
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
                if engine.last_lsn() < lsn {
                    engine.write(lsn, batch);
                }
            }
        }
    }

    /// Returns the minimum last LSN among all engines.
    fn min_last_lsn(&self) -> u64 {
        self.0.values().map(|e| e.last_lsn()).min().unwrap_or(0)
    }

    /// Returns the maximum last LSN among all engines.
    fn max_last_lsn(&self) -> u64 {
        self.0.values().map(|e| e.last_lsn()).max().unwrap_or(0)
    }
}

struct Recover {
    root: RootDir,
    engines: Engines,
    last_lsn: u64,
}

impl Recover {
    fn new(root: RootDir, engines: Engines) -> Self {
        Self {
            root,
            engines,
            last_lsn: 0,
        }
    }

    fn recover(&mut self) -> Result<()> {
        let journals = self.journals_to_recover()?;
        for id in journals.iter().cloned() {
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
                so we can not recover to a consistent state",
                self.last_lsn, max_lsn,
            ));
        }

        // TODO: flush engines.
        for id in journals {
            self.root.delete_journal(id)?;
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

/// A batch of updates to the database.
#[derive(Clone, Default)]
pub struct WriteBatch {
    engines: HashMap<u64, Vec<u8>>,
}

impl WriteBatch {
    /// Creates a write batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a write batch for the bucket.
    pub fn bucket<B: Bucket>(&mut self, bucket: &B) -> B::WriteBatch<'_> {
        let buf = self
            .engines
            .entry(bucket.engine_id())
            .or_insert_with(|| Vec::with_capacity(4096));
        B::WriteBatch::new(bucket.id(), buf)
    }
}

impl AsRef<[u8]> for WriteBatch {
    fn as_ref(&self) -> &[u8] {
        todo!()
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
