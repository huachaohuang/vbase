use std::alloc;
use std::alloc::Layout;
use std::alloc::LayoutError;
use std::alloc::handle_alloc_error;
use std::mem;
use std::num::NonZero;

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
    pub fn with_size(size: usize) -> Result<Self, LayoutError> {
        let mut this = Self::new();
        if let Some(size) = NonZero::new(size) {
            this.alloc(size)?;
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
        let Some(size) = NonZero::new(new_size) else {
            mem::take(self);
            return Ok(());
        };
        if self.size == 0 {
            return self.alloc(size);
        }
        let new_layout = Layout::from_size_align(new_size, ALIGN)?;
        let new_ptr = unsafe { alloc::realloc(self.ptr, self.layout(), new_size) };
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
    fn alloc(&mut self, size: NonZero<usize>) -> Result<(), LayoutError> {
        let size = size.get();
        let layout = Layout::from_size_align(size, ALIGN)?;
        let ptr = unsafe { alloc::alloc(layout) };
        if ptr.is_null() {
            handle_alloc_error(layout);
        }
        self.ptr = ptr;
        self.size = size;
        Ok(())
    }

    fn layout(&self) -> Layout {
        unsafe { Layout::from_size_align_unchecked(self.size, ALIGN) }
    }
}

unsafe impl<const ALIGN: usize> Send for Buffer<ALIGN> {}
unsafe impl<const ALIGN: usize> Sync for Buffer<ALIGN> {}

impl<const ALIGN: usize> Drop for Buffer<ALIGN> {
    fn drop(&mut self) {
        if self.size != 0 {
            unsafe {
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
