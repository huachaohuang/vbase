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

impl ValueKind {
    /// An invalid value kind that can be used as a special marker.
    pub(crate) const NONE: u8 = 0;
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
}
