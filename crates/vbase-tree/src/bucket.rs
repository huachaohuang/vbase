use vbase_engine::engine;
use vbase_engine::engine::internal;
use vbase_engine::util::codec::Encoder;
use vbase_engine::util::sync::Arc;

use crate::data::WriteBatch;
use crate::data::WriteRecord;

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
