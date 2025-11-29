use std::alloc;
use std::alloc::Layout;
use std::alloc::LayoutError;
use std::alloc::handle_alloc_error;

/// A buffer allocated with a specific alignment.
///
/// `ALIGN` specifies the alignment, which must be a power of two.
pub struct Buffer<const ALIGN: usize = 1> {
    ptr: *mut u8,
    size: usize,
}

impl<const ALIGN: usize> Buffer<ALIGN> {
    /// Creates a null buffer without allocation.
    pub const fn new() -> Self {
        assert!(ALIGN.is_power_of_two());
        Self {
            ptr: ALIGN as _,
            size: 0,
        }
    }

    /// Allocates a buffer with the given `size`.
    ///
    /// # Aborts
    ///
    /// Aborts if the allocation fails because of OOM.
    ///
    /// # Errors
    ///
    /// Returns an error if `size` and `ALIGN` do not form a valid [`Layout`].
    pub fn alloc(size: usize) -> Result<Self, LayoutError> {
        if size == 0 {
            return Ok(Self::new());
        }
        let layout = Layout::from_size_align(size, ALIGN)?;
        unsafe {
            let ptr = alloc::alloc(layout);
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            Ok(Self { ptr, size })
        }
    }

    /// Returns the pointer to the buffer.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Returns the mutable pointer to the buffer.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    /// Returns the size of the buffer.
    pub fn size(&self) -> usize {
        self.size
    }
}

unsafe impl<const ALIGN: usize> Send for Buffer<ALIGN> {}
unsafe impl<const ALIGN: usize> Sync for Buffer<ALIGN> {}

impl<const ALIGN: usize> Drop for Buffer<ALIGN> {
    fn drop(&mut self) {
        if self.size != 0 {
            unsafe {
                let layout = Layout::from_size_align_unchecked(self.size, ALIGN);
                alloc::dealloc(self.ptr, layout);
            }
        }
    }
}

impl<const ALIGN: usize> Default for Buffer<ALIGN> {
    fn default() -> Self {
        Self::new()
    }
}
