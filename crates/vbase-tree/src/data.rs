use std::cmp::Ordering;

use vbase_engine::util::codec::Decode;
use vbase_engine::util::codec::Decoder;
use vbase_engine::util::codec::Encode;
use vbase_engine::util::codec::Encoder;
use vbase_engine::util::codec::Varint;

/// A version id.
///
/// Vids are ordered by their id, and then by their LSN in descending order.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct Vid<'a> {
    pub(crate) id: &'a [u8],
    pub(crate) lsn: u64,
}

impl<'a> Vid<'a> {
    /// The minimum version id.
    pub(crate) const MIN: Vid<'static> = Vid {
        id: &[],
        lsn: u64::MAX,
    };

    pub(crate) const fn new(id: &'a [u8], lsn: u64) -> Self {
        Self { id, lsn }
    }
}

impl<'a> Ord for Vid<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        let ord = self.id.cmp(other.id);
        if ord == Ordering::Equal {
            other.lsn.cmp(&self.lsn)
        } else {
            ord
        }
    }
}

impl<'a> PartialOrd for Vid<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Encode for Vid<'a> {
    fn size(&self) -> usize {
        self.id.size() + Varint::size(self.lsn)
    }

    fn encode_to<E: Encoder>(self, enc: &mut E) {
        enc.encode(self.id);
        enc.encode_varint(self.lsn);
    }
}

impl<'a> Decode<'a> for Vid<'a> {
    fn decode_from<D: Decoder<'a>>(dec: &mut D) -> Self {
        let id = dec.decode();
        let lsn = dec.decode_varint();
        Self { id, lsn }
    }
}

/// An owned version id.
pub(crate) struct OwnedVid {
    pub(crate) id: Vec<u8>,
    pub(crate) lsn: u64,
}

impl OwnedVid {
    pub(crate) fn new(id: Vec<u8>, lsn: u64) -> Self {
        Self { id, lsn }
    }

    pub(crate) fn min() -> Self {
        Vid::MIN.into()
    }

    pub(crate) fn set(&mut self, vid: Vid) {
        self.id.clear();
        self.id.extend_from_slice(vid.id);
        self.lsn = vid.lsn;
    }

    pub(crate) fn borrow(&self) -> Vid<'_> {
        Vid::new(&self.id, self.lsn)
    }
}

impl<'a> From<Vid<'a>> for OwnedVid {
    fn from(vid: Vid<'a>) -> Self {
        OwnedVid::new(vid.id.into(), vid.lsn)
    }
}

/// A version value.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Value<'a> {
    Value(&'a [u8]),
    Tombstone,
}

impl<'a> Encode for Value<'a> {
    fn size(&self) -> usize {
        1 + match self {
            Value::Value(v) => v.size(),
            Value::Tombstone => 0,
        }
    }

    fn encode_to<E: Encoder>(self, enc: &mut E) {
        match self {
            Value::Value(v) => {
                enc.encode(ValueKind::Value);
                enc.encode(v);
            }
            Value::Tombstone => {
                enc.encode(ValueKind::Tombstone);
            }
        }
    }
}

impl<'a> Decode<'a> for Value<'a> {
    fn decode_from<D: Decoder<'a>>(dec: &mut D) -> Self {
        match dec.decode::<ValueKind>() {
            ValueKind::Value => Value::Value(dec.decode()),
            ValueKind::Tombstone => Value::Tombstone,
        }
    }
}

/// DO NOT change the values in this enum.
#[repr(u8)]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum ValueKind {
    Value = 1,
    // Merge = 2,
    Tombstone = 3,
}

impl From<u8> for ValueKind {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Value,
            // 2 => Self::Merge,
            3 => Self::Tombstone,
            x => panic!("invalid value kind: {x}"),
        }
    }
}

impl Encode for ValueKind {
    fn size(&self) -> usize {
        1
    }

    fn encode_to<E: Encoder>(self, enc: &mut E) {
        enc.put(self as u8);
    }
}

impl<'de> Decode<'de> for ValueKind {
    fn decode_from<D: Decoder<'de>>(dec: &mut D) -> Self {
        dec.pop().into()
    }
}

/// A batch of updates.
pub(crate) struct WriteBatch<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'a> WriteBatch<'a> {
    pub(crate) fn new(buf: &'a mut Vec<u8>) -> Self {
        Self { buf }
    }

    pub(crate) fn add(&mut self, record: WriteRecord<'a>) {
        record.encode_to(self.buf);
    }
}

impl<'a> Drop for WriteBatch<'a> {
    fn drop(&mut self) {
        WriteRecord::encode_end(self.buf);
    }
}

/// An iterator over a write batch.
pub(crate) struct WriteBatchIter<'a> {
    buf: &'a [u8],
}

impl<'a> WriteBatchIter<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }
}

impl<'a> Iterator for WriteBatchIter<'a> {
    type Item = WriteRecord<'a>;

    fn next(&mut self) -> Option<Self::Item> {
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
    const END: u8 = 0;

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
            ValueKind::Tombstone => {
                let id = dec.decode();
                Some(Self::Tombstone(id))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use vbase_engine::util::codec;

    use super::*;

    #[test]
    fn test_vid() {
        assert!(Vid::new(b"", 0) > Vid::MIN);
        assert!(Vid::new(b"1", 1) > Vid::new(b"1", 2));
        assert!(Vid::new(b"1", 2) < Vid::new(b"2", 1));
        codec::test_value(Vid::new(b"test", 123));
    }

    #[test]
    fn test_value() {
        codec::test_value(Value::Value(b"test"));
        codec::test_value(Value::Tombstone);
    }

    #[test]
    fn test_write_batch() {
        const K1: &[u8] = b"K1";
        const K2: &[u8] = b"K2";

        let mut buf = Vec::new();
        {
            let mut batch = WriteBatch::new(&mut buf);
            batch.add(WriteRecord::Value(K1, K1));
            batch.add(WriteRecord::Tombstone(K1));
        }
        {
            let mut batch = WriteBatch::new(&mut buf);
            batch.add(WriteRecord::Value(K2, K2));
            batch.add(WriteRecord::Tombstone(K2));
        }

        let mut iter = WriteBatchIter::new(&buf);
        assert_eq!(iter.next(), Some(WriteRecord::Value(K1, K1)));
        assert_eq!(iter.next(), Some(WriteRecord::Tombstone(K1)));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), Some(WriteRecord::Value(K2, K2)));
        assert_eq!(iter.next(), Some(WriteRecord::Tombstone(K2)));
        assert_eq!(iter.next(), None);
    }
}
