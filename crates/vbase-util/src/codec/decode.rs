use std::slice;

use super::Decoder;

/// A helper trait to implement [`Decoder`].
trait Take {
    /// Takes `len` bytes and returns a pointer to the start of the taken bytes.
    fn take(&mut self, len: usize) -> *const u8;
}

impl<'de, T: Take> Decoder<'de> for T {
    fn pop(&mut self) -> u8 {
        let ptr = self.take(1);
        unsafe { ptr.read() }
    }

    fn remove(&mut self, len: usize) -> &'de [u8] {
        let ptr = self.take(len);
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

impl Take for &[u8] {
    fn take(&mut self, len: usize) -> *const u8 {
        assert!(self.len() >= len);
        let ptr = self.as_ptr();
        unsafe {
            *self = slice::from_raw_parts(ptr.add(len), self.len() - len);
        }
        ptr
    }
}

/// An unsafe decoder that reads from a raw pointer.
pub struct UnsafeDecoder {
    ptr: *const u8,
}

impl UnsafeDecoder {
    /// Creates a new [`UnsafeDecoder`] from a raw pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the pointer is valid for reads.
    pub unsafe fn new(ptr: *const u8) -> UnsafeDecoder {
        UnsafeDecoder { ptr }
    }
}

impl Take for UnsafeDecoder {
    fn take(&mut self, len: usize) -> *const u8 {
        let ptr = self.ptr;
        unsafe {
            self.ptr = self.ptr.add(len);
        };
        ptr
    }
}
