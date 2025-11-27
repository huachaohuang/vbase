use vbase_engine::engine;
use vbase_engine::engine::internal;
use vbase_engine::util::codec::Decoder;
use vbase_engine::util::codec::Encoder;
use vbase_engine::util::sync::Arc;

use crate::data::ValueKind;

#[derive(Debug)]
pub struct Bucket(Arc<BucketHandle>);

impl engine::Bucket for Bucket {
    type WriteBatch<'a> = WriteBatch<'a>;
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

pub struct WriteBatch<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'a> WriteBatch<'a> {
    fn new(id: u64, buf: &'a mut Vec<u8>) -> Self {
        buf.encode_varint(id);
        Self { buf }
    }

    pub fn put(&mut self, id: &[u8], value: &[u8]) -> &mut Self {
        let record = WriteRecord::Value(id, value);
        record.encode_to(self.buf);
        self
    }

    pub fn delete(&mut self, id: &[u8]) -> &mut Self {
        let record = WriteRecord::Tombstone(id);
        record.encode_to(self.buf);
        self
    }
}

impl<'a> Drop for WriteBatch<'a> {
    fn drop(&mut self) {
        WriteRecord::encode_end(self.buf);
    }
}

impl<'a> internal::WriteBatch<'a> for WriteBatch<'a> {
    fn new(id: u64, buf: &'a mut Vec<u8>) -> Self {
        Self::new(id, buf)
    }
}

/// An iterator over the write batch.
pub(crate) struct WriteBatchIter<'a> {
    buf: &'a [u8],
}

impl<'a> WriteBatchIter<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    /// Returns the next bucket id.
    pub(crate) fn next_bucket(&mut self) -> Option<u64> {
        if self.buf.is_empty() {
            return None;
        }
        Some(self.buf.decode_varint())
    }

    /// Returns the next record in the current bucket.
    pub(crate) fn next_record(&mut self) -> Option<WriteRecord<'a>> {
        WriteRecord::decode_from(&mut self.buf)
    }
}

/// A record in the write batch.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum WriteRecord<'a> {
    Value(&'a [u8], &'a [u8]),
    Tombstone(&'a [u8]),
}

impl<'a> WriteRecord<'a> {
    /// The end marker.
    const END: u8 = ValueKind::NONE;

    fn encode_to<E: Encoder>(self, enc: &mut E) {
        match self {
            Self::Value(id, value) => {
                enc.encode(ValueKind::Value);
                enc.encode(id);
                enc.encode(value);
            }
            Self::Tombstone(id) => {
                enc.encode(ValueKind::Tombstone);
                enc.encode(id);
            }
        }
    }

    fn encode_end<E: Encoder>(enc: &mut E) {
        enc.put(Self::END);
    }

    fn decode_from<D: Decoder<'a>>(dec: &mut D) -> Option<Self> {
        let kind = dec.pop();
        if kind == Self::END {
            return None;
        }
        match ValueKind::from(kind) {
            ValueKind::Value => {
                let id = dec.decode();
                let value = dec.decode();
                Some(Self::Value(id, value))
            }
            ValueKind::Merge => todo!(),
            ValueKind::Tombstone => {
                let id = dec.decode();
                Some(Self::Tombstone(id))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_batch() {
        const K1: &[u8] = b"1";
        const K2: &[u8] = b"2";
        let mut buf = Vec::new();
        WriteBatch::new(1, &mut buf).put(K1, K1).delete(K1);
        WriteBatch::new(2, &mut buf).put(K2, K2).delete(K2);
        let mut iter = WriteBatchIter::new(&buf);
        assert_eq!(iter.next_bucket(), Some(1));
        assert_eq!(iter.next_record(), Some(WriteRecord::Value(K1, K1)));
        assert_eq!(iter.next_record(), Some(WriteRecord::Tombstone(K1)));
        assert_eq!(iter.next_record(), None);
        assert_eq!(iter.next_bucket(), Some(2));
        assert_eq!(iter.next_record(), Some(WriteRecord::Value(K2, K2)));
        assert_eq!(iter.next_record(), Some(WriteRecord::Tombstone(K2)));
        assert_eq!(iter.next_record(), None);
        assert_eq!(iter.next_bucket(), None);
    }
}
