mod encode;
pub use encode::BytesEncoder;
pub use encode::UnsafeEncoder;

mod decode;
pub use decode::UnsafeDecoder;

/// A variable-length integer.
pub trait Varint {
    /// The maximum encoded size.
    const MAX_VARINT_SIZE: usize;

    /// Returns the size of the encoded value.
    fn size(self) -> usize;

    /// Encodes the value to the given encoder.
    fn encode_to<E: Encoder>(self, enc: &mut E);

    /// Decodes the value from the given decoder.
    fn decode_from<'de, D: Decoder<'de>>(dec: &mut D) -> Self;
}

/// A value that can be encoded to [`Encoder`].
pub trait Encode {
    /// Returns the size of the encoded value.
    fn size(&self) -> usize;

    /// Encodes the value to the given encoder.
    fn encode_to<E: Encoder>(self, enc: &mut E);
}

/// A value that can be decoded from [`Decoder`].
pub trait Decode<'de> {
    /// Decodes the value from the given decoder.
    fn decode_from<D: Decoder<'de>>(dec: &mut D) -> Self;
}

macro_rules! impl_int {
    ($t:ty) => {
        impl Varint for $t {
            const MAX_VARINT_SIZE: usize = <$t>::BITS as usize / 7 + 1;

            fn size(self) -> usize {
                let mut n = 1;
                let mut v = self;
                while v >= 0x80 {
                    n += 1;
                    v >>= 7;
                }
                n
            }

            fn encode_to<E: Encoder>(self, enc: &mut E) {
                let mut v = self;
                while v >= 0x80 {
                    enc.put(v as u8);
                    v >>= 7;
                }
                enc.put(v as u8);
            }

            fn decode_from<'de, D: Decoder<'de>>(dec: &mut D) -> Self {
                let mut value = 0;
                let mut shift = 0;
                while shift < <$t>::BITS {
                    let b = dec.pop();
                    if b >= 0x80 {
                        value |= ((b & 0x7F) as Self) << shift;
                        shift += 7;
                    } else {
                        return value | (b as Self) << shift;
                    }
                }
                panic!("varint overflow {}", std::any::type_name::<Self>());
            }
        }

        impl Encode for $t {
            fn size(&self) -> usize {
                size_of::<$t>()
            }

            fn encode_to<E: Encoder>(self, enc: &mut E) {
                enc.append(&self.to_le_bytes());
            }
        }

        impl<'de> Decode<'de> for $t {
            fn decode_from<D: Decoder<'de>>(dec: &mut D) -> Self {
                let bytes = dec.remove(size_of::<$t>());
                <$t>::from_le_bytes(unsafe { *bytes.as_ptr().cast() })
            }
        }
    };

    ($a:ty, $($b:ty),+) => {
        impl_int!($a);
        impl_int!($($b),+);
    };
}

impl_int!(u16, u32, u64, usize);

impl Encode for u8 {
    fn size(&self) -> usize {
        1
    }

    fn encode_to<E: Encoder>(self, enc: &mut E) {
        enc.put(self);
    }
}

impl<'de> Decode<'de> for u8 {
    fn decode_from<D: Decoder<'de>>(dec: &mut D) -> Self {
        dec.pop()
    }
}

impl Encode for &[u8] {
    fn size(&self) -> usize {
        Varint::size(self.len()) + self.len()
    }

    fn encode_to<E: Encoder>(self, enc: &mut E) {
        enc.encode_varint(self.len());
        enc.append(self);
    }
}

impl<'de> Decode<'de> for &'de [u8] {
    fn decode_from<D: Decoder<'de>>(dec: &mut D) -> Self {
        let len = dec.decode_varint();
        dec.remove(len)
    }
}

/// A buffer that can encode values to.
pub trait Encoder: Sized {
    /// Puts a byte to the encoder.
    fn put(&mut self, byte: u8);

    /// Appends some bytes to the encoder.
    fn append(&mut self, bytes: &[u8]);

    /// Encodes a value to the encoder.
    fn encode<T: Encode>(&mut self, value: T) {
        value.encode_to(self)
    }

    /// Encodes a variable-length integer to the encoder.
    fn encode_varint<T: Varint>(&mut self, value: T) {
        value.encode_to(self)
    }
}

/// A buffer that can decode values from.
pub trait Decoder<'de>: Sized {
    /// Pops a byte from the decoder.
    fn pop(&mut self) -> u8;

    /// Removes `len` bytes from the decoder.
    fn remove(&mut self, len: usize) -> &'de [u8];

    /// Decodes a value from the decoder.
    fn decode<T: Decode<'de>>(&mut self) -> T {
        T::decode_from(self)
    }

    /// Decodes a variable-length integer from the decoder.
    fn decode_varint<T: Varint>(&mut self) -> T {
        T::decode_from(self)
    }
}

/// A helper function to test codec of a value.
#[cfg(any(test, feature = "test"))]
pub fn test_value<'de, T>(value: T)
where
    T: Encode + Decode<'de> + Clone + PartialEq + std::fmt::Debug,
{
    let mut enc = Vec::new();
    enc.encode(value.clone());
    assert_eq!(enc.len(), value.size());
    let mut dec: &[u8] = unsafe {
        // SAFETY: extend lifetime within this function
        std::mem::transmute(enc.as_slice())
    };
    assert_eq!(dec.decode::<T>(), value);
}

#[cfg(test)]
mod tests {
    use super::*;

    const BYTES: &[u8] = b"test";

    fn encode<T: Encoder>(mut enc: T) {
        enc.encode(u8::MAX);
        enc.encode(u16::MAX);
        enc.encode(u32::MAX);
        enc.encode(u64::MAX);
        enc.encode(usize::MAX);
        enc.encode(BYTES);
        enc.encode_varint(u16::MAX);
        enc.encode_varint(u32::MAX);
        enc.encode_varint(u64::MAX);
        enc.encode_varint(usize::MAX);
    }

    fn decode<'de, T: Decoder<'de>>(mut dec: T) {
        assert_eq!(dec.decode::<u8>(), u8::MAX);
        assert_eq!(dec.decode::<u16>(), u16::MAX);
        assert_eq!(dec.decode::<u32>(), u32::MAX);
        assert_eq!(dec.decode::<u64>(), u64::MAX);
        assert_eq!(dec.decode::<usize>(), usize::MAX);
        assert_eq!(dec.decode::<&[u8]>(), BYTES);
        assert_eq!(dec.decode_varint::<u16>(), u16::MAX);
        assert_eq!(dec.decode_varint::<u32>(), u32::MAX);
        assert_eq!(dec.decode_varint::<u64>(), u64::MAX);
        assert_eq!(dec.decode_varint::<usize>(), usize::MAX);
    }

    #[test]
    fn test_slice() {
        let mut buf = [0; 1024];
        encode(buf.as_mut_slice());
        decode(buf.as_slice());
    }

    #[test]
    fn test_bytes() {
        let mut buf = [0; 1024];
        let mut enc = BytesEncoder::new(&mut buf);
        enc.encode(BYTES);
        let mut dec = enc.encoded_bytes();
        assert_eq!(dec.decode::<&[u8]>(), BYTES);
    }

    #[test]
    fn test_unsafe() {
        let mut buf = [0; 1024];
        encode(unsafe { UnsafeEncoder::new(buf.as_mut_ptr()) });
        decode(unsafe { UnsafeDecoder::new(buf.as_ptr()) });
    }

    #[test]
    fn test_varint() {
        assert_eq!(u16::MAX_VARINT_SIZE, 3);
        assert_eq!(u32::MAX_VARINT_SIZE, 5);
        assert_eq!(u64::MAX_VARINT_SIZE, 10);
        assert_eq!(Varint::size(u16::MAX), u16::MAX_VARINT_SIZE);
        assert_eq!(Varint::size(u32::MAX), u32::MAX_VARINT_SIZE);
        assert_eq!(Varint::size(u64::MAX), u64::MAX_VARINT_SIZE);
        assert_eq!(Varint::size(usize::MAX), usize::MAX_VARINT_SIZE);
    }
}
