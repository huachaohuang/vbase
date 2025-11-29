use std::collections::HashMap;

use crossbeam_epoch as epoch;
use crossbeam_epoch::Atomic;
use crossbeam_epoch::Guard;
use log::info;
use vbase_engine::engine;
use vbase_engine::engine::internal;
use vbase_engine::engine::internal::BucketHandle as _;
use vbase_engine::env::boxed::Dir;
use vbase_engine::util::sync::Arc;
use vbase_engine::util::sync::Mutex;
use vbase_engine::util::sync::atomic::AtomicU64;
use vbase_engine::util::sync::atomic::Ordering::Relaxed;

use crate::Error;
use crate::Result;
use crate::bucket::Bucket;
use crate::bucket::BucketHandle;
use crate::bucket::WriteBatchIter;
use crate::file::RootDir;
use crate::manifest::BucketDesc;
use crate::manifest::Desc;
use crate::manifest::Edit;
use crate::manifest::Manifest;
use crate::manifest::ManifestWriter;
use crate::memtable::MemTable;

const NAME: &str = "Tree";

pub struct Engine;

impl engine::Engine for Engine {
    type Bucket = Bucket;
}

impl internal::Engine for Engine {
    type Handle = EngineHandle;

    const NAME: &str = NAME;

    fn open(id: u64, dir: Dir) -> Result<Self::Handle> {
        EngineHandle::open(id, dir)
    }
}

pub struct EngineHandle {
    id: u64,
    root: RootDir,

    next_id: AtomicU64,

    current: Atomic<Current>,

    buckets: Mutex<HashMap<String, Arc<BucketHandle>>>,

    manifest: Mutex<ManifestWriter>,
}

impl EngineHandle {
    fn open(id: u64, dir: Dir) -> Result<Self> {
        let root = RootDir::new(dir);

        // Load the current manifest.
        let mut desc = match root.read_current()? {
            Some(id) => {
                info!("recover from manifest {id}");
                root.open_manifest(id).and_then(Manifest::load)?
            }
            None => Desc::default(),
        };

        let mut buckets = HashMap::new();
        for bucket in desc.buckets.clone() {
            let handle = BucketHandle::new(bucket.id, id);
            buckets.insert(bucket.name, handle.into());
        }

        // Switch to a new manifest.
        let last_id = desc.last_id + 1;
        let manifest = root.create_manifest(last_id).and_then(|file| {
            desc.last_id = last_id;
            ManifestWriter::open(desc, file)
        })?;
        root.switch_current(last_id)?;

        // Clean up obsolete files.
        let list = root.list()?;
        for id in list.manifests {
            if id != last_id {
                root.delete_manifest(id)?;
            }
        }

        Ok(Self {
            id,
            root,
            next_id: AtomicU64::new(last_id + 1),
            current: Atomic::new(Current::new()),
            buckets: Mutex::new(buckets),
            manifest: Mutex::new(manifest),
        })
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Relaxed)
    }

    fn current<'g>(&self, guard: &'g Guard) -> &'g Current {
        unsafe { self.current.load(Relaxed, guard).deref() }
    }

    fn update_manifest(&self, edit: Edit) -> Result<()> {
        let mut manifest = self.manifest.lock().unwrap();
        manifest.write(edit)?;
        if manifest.should_switch_file() {
            let id = self.next_id();
            let file = self.root.create_manifest(id)?;
            manifest.switch_file(id, file)?;
            self.root.switch_current(id)?;
            self.root.delete_manifest(id)?;
        }
        Ok(())
    }
}

impl internal::EngineHandle for EngineHandle {
    fn id(&self) -> u64 {
        self.id
    }

    fn name(&self) -> &str {
        NAME
    }

    fn write(&self, lsn: u64, batch: &[u8]) {
        let guard = &epoch::pin();
        let current = self.current(guard);
        let mut iter = WriteBatchIter::new(batch);
        while let Some(id) = iter.next_bucket() {
            let Some(bucket) = current.mem.bucket(id) else {
                // Ignore deleted buckets.
                continue;
            };
            while let Some(record) = iter.next_record() {
                todo!()
            }
        }
    }

    fn last_lsn(&self) -> u64 {
        0
    }

    fn bucket(&self, name: &str) -> Result<Arc<dyn internal::BucketHandle>> {
        let buckets = self.buckets.lock().unwrap();
        let Some(bucket) = buckets.get(name) else {
            return Err(Error::NotExist(format!("bucket {name}")));
        };
        Ok(bucket.clone())
    }

    fn create_bucket(&self, name: &str) -> Result<Arc<dyn internal::BucketHandle>> {
        let mut buckets = self.buckets.lock().unwrap();
        if buckets.contains_key(name) {
            return Err(Error::Exists(format!("bucket {name}")));
        }

        let id = self.next_id();
        let desc = BucketDesc {
            id,
            name: name.into(),
        };
        info!("create bucket {name} with id {id}");
        let mut edit = Edit::default();
        edit.last_id = id;
        edit.add_buckets.push(desc);
        self.update_manifest(edit)?;

        let guard = &epoch::pin();
        let current = self.current(guard);
        current.mem.add_bucket(id);

        let bucket = Arc::new(BucketHandle::new(id, self.id));
        buckets.insert(name.into(), bucket.clone());
        Ok(bucket)
    }

    fn delete_bucket(&self, name: &str) -> Result<()> {
        let mut buckets = self.buckets.lock().unwrap();
        let Some(bucket) = buckets.get(name) else {
            return Err(Error::NotExist(format!("bucket {name}")));
        };

        info!("delete bucket {name}");
        let mut edit = Edit::default();
        edit.delete_buckets.push(bucket.id());
        self.update_manifest(edit)?;

        buckets.remove(name);
        Ok(())
    }
}

struct Current {
    mem: MemTable,
}

impl Current {
    fn new() -> Self {
        Self {
            mem: MemTable::new(1024),
        }
    }
}
