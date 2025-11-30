use std::slice;

use super::Encoder;

impl Encoder for Vec<u8> {
    fn put(&mut self, byte: u8) {
        self.push(byte);
    }

    fn append(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

/// A helper trait to implement [`Encoder`].
trait Take {
    /// Takes `len` bytes and returns a pointer to the start of the taken bytes.
    fn take(&mut self, len: usize) -> *mut u8;
}

impl<T: Take> Encoder for T {
    fn put(&mut self, byte: u8) {
        let ptr = self.take(1);
        unsafe {
            ptr.write(byte);
        }
    }

    fn append(&mut self, bytes: &[u8]) {
        let ptr = self.take(bytes.len());
        unsafe {
            ptr.copy_from_nonoverlapping(bytes.as_ptr(), bytes.len());
        }
    }
}

impl Take for &mut [u8] {
    fn take(&mut self, len: usize) -> *mut u8 {
        assert!(self.len() >= len);
        let ptr = self.as_mut_ptr();
        unsafe {
            *self = slice::from_raw_parts_mut(ptr.add(len), self.len() - len);
        }
        ptr
    }
}

/// An encoder that writes to a byte slice.
pub struct BytesEncoder<'a> {
    buf: &'a mut [u8],
    len: usize,
}

impl BytesEncoder<'_> {
    /// Creates a new [`BytesEncoder`] from a byte slice.
    pub fn new<T: AsMut<[u8]>>(buf: &mut T) -> BytesEncoder<'_> {
        BytesEncoder {
            buf: buf.as_mut(),
            len: 0,
        }
    }

    /// Returns the encoded bytes.
    pub fn encoded_bytes(&self) -> &[u8] {
        unsafe { self.buf.get_unchecked(..self.len) }
    }
}

impl Take for BytesEncoder<'_> {
    fn take(&mut self, len: usize) -> *mut u8 {
        assert!(self.buf.len() - self.len >= len);
        let ptr = unsafe { self.buf.as_mut_ptr().add(self.len) };
        self.len += len;
        ptr
    }
}

/// An unsafe encoder that writes to a raw pointer.
pub struct UnsafeEncoder {
    ptr: *mut u8,
}

impl UnsafeEncoder {
    /// Creates a new [`UnsafeEncoder`] from a raw pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the pointer is valid for writes.
    pub unsafe fn new(ptr: *mut u8) -> UnsafeEncoder {
        UnsafeEncoder { ptr }
    }
}

impl Take for UnsafeEncoder {
    fn take(&mut self, len: usize) -> *mut u8 {
        let ptr = self.ptr;
        unsafe {
            self.ptr = self.ptr.add(len);
        }
        ptr
    }
}
