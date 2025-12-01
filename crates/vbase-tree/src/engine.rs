use std::collections::HashMap;

use log::info;
use vbase_engine::engine;
use vbase_engine::engine::internal;
use vbase_engine::engine::internal::BucketHandle as _;
use vbase_engine::env::boxed::Dir;
use vbase_engine::util::codec::Encoder;
use vbase_engine::util::sync::Arc;
use vbase_engine::util::sync::Mutex;
use vbase_engine::util::sync::atomic::AtomicU64;
use vbase_engine::util::sync::atomic::Ordering::Relaxed;

use crate::Error;
use crate::Result;
use crate::data::WriteBatch;
use crate::data::WriteRecord;
use crate::file::RootDir;
use crate::manifest::BucketDesc;
use crate::manifest::Desc;
use crate::manifest::Edit;
use crate::manifest::Manifest;
use crate::manifest::ManifestWriter;

const NAME: &str = "Tree";

#[derive(Debug)]
pub struct Bucket(Arc<BucketHandle>);

impl engine::Bucket for Bucket {
    type Reader<'a> = Reader<'a>;
    type Writer<'a> = Writer<'a>;
}

impl internal::Bucket for Bucket {
    type Handle = BucketHandle;

    fn open(handle: Arc<Self::Handle>) -> Self {
        Bucket(handle)
    }

    fn handle(&self) -> &Self::Handle {
        &self.0
    }
}

#[derive(Debug)]
pub struct BucketHandle {
    id: u64,
    engine_id: u64,
}

impl BucketHandle {
    pub(crate) fn new(id: u64, engine_id: u64) -> Self {
        Self { id, engine_id }
    }
}

impl internal::BucketHandle for BucketHandle {
    fn id(&self) -> u64 {
        self.id
    }

    fn engine_id(&self) -> u64 {
        self.engine_id
    }
}

pub struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> internal::Reader<'a> for Reader<'a> {
    fn new(_: u64) -> Self {
        todo!()
    }
}

pub struct Writer<'a>(WriteBatch<'a>);

impl<'a> Writer<'a> {
    fn new(id: u64, buf: &'a mut Vec<u8>) -> Self {
        buf.encode_varint(id);
        Self(WriteBatch::new(buf))
    }

    pub fn put(&mut self, id: &[u8], value: &[u8]) -> &mut Self {
        self.0.add(WriteRecord::Value(id, value));
        self
    }

    pub fn delete(&mut self, id: &[u8]) -> &mut Self {
        self.0.add(WriteRecord::Tombstone(id));
        self
    }
}

impl<'a> internal::Writer<'a> for Writer<'a> {
    fn new(id: u64, buf: &'a mut Vec<u8>) -> Self {
        Self::new(id, buf)
    }
}

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

    buckets: Mutex<HashMap<String, Arc<BucketHandle>>>,

    manifest: Mutex<ManifestWriter>,
}

impl EngineHandle {
    fn open(engine_id: u64, dir: Dir) -> Result<Self> {
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
        for (&id, bucket) in &desc.buckets {
            let handle = BucketHandle::new(id, engine_id);
            buckets.insert(bucket.name.clone(), handle.into());
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
            id: engine_id,
            root,
            next_id: AtomicU64::new(last_id + 1),
            buckets: Mutex::new(buckets),
            manifest: Mutex::new(manifest),
        })
    }

    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Relaxed)
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
        todo!()
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
        let desc = BucketDesc::new(name.into());
        info!("create bucket {name} with id {id}");
        let mut edit = Edit::default();
        edit.last_id = id;
        edit.add_buckets.insert(id, desc);
        self.update_manifest(edit)?;

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
