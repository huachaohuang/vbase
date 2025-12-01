use std::ops::Deref;
use std::ops::DerefMut;
use std::slice;

use crate::alloc::Buffer;
use crate::codec::Encoder;

/// A bytes vector with a specified alignment.
///
/// `ALIGN` specifies the alignment, which must be a power of two.
pub struct BytesVec<const ALIGN: usize = 1> {
    buf: Buffer<ALIGN>,
    len: usize,
}

impl<const ALIGN: usize> BytesVec<ALIGN> {
    /// Creates a new [`BytesVec`].
    pub const fn new() -> Self {
        Self {
            buf: Buffer::new(),
            len: 0,
        }
    }

    /// Returns the number of bytes in the vector.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the vector is empty.
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns a pointer to the vector.
    pub const fn as_ptr(&self) -> *const u8 {
        self.buf.as_ptr()
    }

    /// Returns a mutable pointer to the vector.
    pub const fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    /// Returns a slice of the vector.
    pub const fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len) }
    }

    /// Returns a mutable slice of in the vector.
    pub const fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.len) }
    }

    /// Pushes a byte to the end of the vector.
    pub fn push(&mut self, value: u8) {
        self.reserve(1);
        unsafe {
            // SAFETY: `self.buf` has at least `self.len + 1` bytes.
            self.as_mut_ptr().add(self.len).write(value);
            self.len += 1;
        }
    }

    /// Fills the vector to the next alignment with the given value.
    ///
    /// Returns the number of bytes added.
    pub fn fill_to_align(&mut self, value: u8) -> usize {
        let len = ALIGN - (self.len % ALIGN);
        self.append(&[value; ALIGN][..len]);
        len
    }

    /// Extends the vector with the given slice.
    pub fn extend_from_slice(&mut self, slice: &[u8]) {
        self.reserve(slice.len());
        unsafe {
            // SAFETY: `self.buf` has at least `self.len + slice.len()` bytes.
            self.as_mut_ptr()
                .add(self.len)
                .copy_from_nonoverlapping(slice.as_ptr(), slice.len());
            self.len += slice.len();
        }
    }

    /// Removes all bytes from the vector.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Reserves for at least `additional` more bytes to be inserted.
    ///
    /// This function over-allocates to amortize the allocation cost.
    pub fn reserve(&mut self, additional: usize) {
        if self.buf.size() - self.len >= additional {
            return;
        }
        self.grow(additional, true);
    }

    /// Same as [`Self::reserve`], but does not over-allocate.
    pub fn reserve_exact(&mut self, additional: usize) {
        if self.buf.size() - self.len >= additional {
            return;
        }
        self.grow(additional, false);
    }
}

impl<const ALIGN: usize> BytesVec<ALIGN> {
    #[cold]
    fn grow(&mut self, additional: usize, amortized: bool) {
        let new_size = self.len.strict_add(additional);
        let new_size = if amortized {
            new_size.max(self.buf.size() * 2)
        } else {
            new_size
        };
        self.buf.realloc(new_size).unwrap();
    }
}

impl<const ALIGN: usize> Clone for BytesVec<ALIGN> {
    fn clone(&self) -> Self {
        let mut vec = Self::new();
        vec.append(self);
        vec
    }
}

impl<const ALIGN: usize> Deref for BytesVec<ALIGN> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<const ALIGN: usize> DerefMut for BytesVec<ALIGN> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<const ALIGN: usize> AsRef<[u8]> for BytesVec<ALIGN> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl<const ALIGN: usize> AsMut<[u8]> for BytesVec<ALIGN> {
    fn as_mut(&mut self) -> &mut [u8] {
        self
    }
}

impl<const ALIGN: usize> Default for BytesVec<ALIGN> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const ALIGN: usize> Encoder for BytesVec<ALIGN> {
    fn put(&mut self, byte: u8) {
        self.push(byte);
    }

    fn append(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_vec() {
        const ALIGN: usize = 8;
        let mut vec = BytesVec::<ALIGN>::new();
        vec.push(1);
        assert_eq!(vec.len, 1);
        assert_eq!(vec.buf.size(), 1);
        assert_eq!(vec.as_ptr().align_offset(ALIGN), 0);
        vec.fill_to_align(1);
        assert_eq!(vec.len, ALIGN);
        assert_eq!(vec.buf.size(), ALIGN);
        assert_eq!(vec.as_ptr().align_offset(ALIGN), 0);
        vec.extend_from_slice(&[1; ALIGN]);
        assert_eq!(vec.len, ALIGN * 2);
        assert_eq!(vec.buf.size(), ALIGN * 2);
        assert_eq!(vec.as_ptr().align_offset(ALIGN), 0);
        vec.reserve_exact(ALIGN);
        assert_eq!(vec.buf.size(), ALIGN * 3);
        assert_eq!(vec.as_slice(), &[1; ALIGN * 2]);
        vec.clear();
        assert_eq!(vec.len, 0);
    }
}
