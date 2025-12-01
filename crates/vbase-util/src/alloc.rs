use std::alloc;
use std::alloc::Layout;
use std::alloc::LayoutError;
use std::alloc::handle_alloc_error;
use std::mem;
use std::ptr;

/// A buffer allocated with a specific alignment.
///
/// The pointer of the buffer is guaranteed to be aligned to `ALIGN`.
pub struct Buffer<const ALIGN: usize = 1> {
    ptr: *mut u8,
    size: usize,
}

impl<const ALIGN: usize> Buffer<ALIGN> {
    /// Creates a zero-sized buffer without allocation.
    pub const fn new() -> Self {
        Self {
            ptr: ptr::without_provenance_mut(ALIGN),
            size: 0,
        }
    }

    /// Creates a buffer with the given `size`.
    ///
    /// # Aborts
    ///
    /// Aborts if the allocation fails because of OOM.
    ///
    /// # Errors
    ///
    /// Returns an error if `size` and `ALIGN` do not form a valid [`Layout`].
    pub fn with_size(size: usize) -> Result<Self, LayoutError> {
        let mut this = Self::new();
        if size != 0 {
            unsafe {
                // SAFETY: `size` is non-zero.
                this.alloc(size)?;
            }
        }
        Ok(this)
    }

    /// Reallocates the buffer to the new size.
    ///
    /// # Aborts
    ///
    /// Aborts if the allocation fails because of OOM.
    ///
    /// # Errors
    ///
    /// Returns an error if `new_size` and `ALIGN` do not form a valid
    /// [`Layout`].
    pub fn realloc(&mut self, new_size: usize) -> Result<(), LayoutError> {
        if new_size == 0 {
            mem::take(self);
            return Ok(());
        }
        if self.size == 0 {
            // SAFETY: `new_size` is non-zero.
            return unsafe { self.alloc(new_size) };
        }
        let new_layout = Layout::from_size_align(new_size, ALIGN)?;
        let new_ptr = unsafe {
            // SAFETY:
            // - `size` is non-zero, so `ptr` and `layout` must be valid.
            // - `new_size` is non-zero, and forms a valid layout with `ALIGN`.
            alloc::realloc(self.ptr, self.layout(), new_size)
        };
        if new_ptr.is_null() {
            handle_alloc_error(new_layout);
        }
        self.ptr = new_ptr;
        self.size = new_size;
        Ok(())
    }

    /// Returns the pointer to the buffer.
    pub const fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Returns the mutable pointer to the buffer.
    pub const fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    /// Returns the size of the buffer.
    pub const fn size(&self) -> usize {
        self.size
    }
}

impl<const ALIGN: usize> Buffer<ALIGN> {
    unsafe fn alloc(&mut self, size: usize) -> Result<(), LayoutError> {
        let layout = Layout::from_size_align(size, ALIGN)?;
        let ptr = unsafe { alloc::alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        self.ptr = ptr;
        self.size = size;
        Ok(())
    }

    unsafe fn layout(&self) -> Layout {
        unsafe { Layout::from_size_align_unchecked(self.size, ALIGN) }
    }
}

unsafe impl<const ALIGN: usize> Send for Buffer<ALIGN> {}
unsafe impl<const ALIGN: usize> Sync for Buffer<ALIGN> {}

impl<const ALIGN: usize> Drop for Buffer<ALIGN> {
    fn drop(&mut self) {
        if self.size != 0 {
            unsafe {
                // SAFETY: `size` is non-zero, so `ptr` and `layout` must be valid.
                alloc::dealloc(self.ptr, self.layout());
            }
        }
    }
}

impl<const ALIGN: usize> Default for Buffer<ALIGN> {
    fn default() -> Self {
        Self::new()
    }
}
