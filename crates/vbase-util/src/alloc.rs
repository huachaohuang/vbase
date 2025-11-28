use std::alloc;
use std::alloc::Layout;
use std::alloc::handle_alloc_error;
use std::ptr::null_mut;

/// A buffer allocated with a specific alignment.
pub struct Buffer<const ALIGN: usize> {
    ptr: *mut u8,
    size: usize,
}

impl<const ALIGN: usize> Buffer<ALIGN> {
    /// Creates a null buffer without allocation.
    pub const fn new() -> Self {
        Self {
            ptr: null_mut(),
            size: 0,
        }
    }

    /// Allocates a buffer with the given `size`.
    ///
    /// # Panics
    ///
    /// Panics if the allocation fails.
    ///
    /// # Errors
    ///
    /// Returns an error if `size` and `ALIGN` do not form a valid layout.
    pub fn alloc(size: usize) -> Result<Self, LayoutError> {
        let layout = layout(size, ALIGN)?;
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
    pub fn as_mut_ptr(&self) -> *mut u8 {
        self.ptr
    }

    /// Returns the size of the buffer.
    pub fn size(&self) -> usize {
        self.size
    }
}

impl<const ALIGN: usize> Drop for Buffer<ALIGN> {
    fn drop(&mut self) {
        if self.size != 0 {
            unsafe {
                let layout = layout_unchecked(self.size, ALIGN);
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

/// An error indicating that a layout could not be created.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct LayoutError(usize, usize);

/// Creates a layout with the given `size` and `align`.
///
/// # Errors
///
/// Returns an error if `size` is zero, in addition to the error conditions in
/// [`Layout::from_size_align`]. This is to prevent allocations with a
/// zero-sized layout, which is Undefined Behavior. See
/// [`alloc::GlobalAlloc::alloc`] for more details.
pub fn layout(size: usize, align: usize) -> Result<Layout, LayoutError> {
    if size == 0 {
        return Err(LayoutError(size, align));
    }
    Layout::from_size_align(size, align).map_err(|_| LayoutError(size, align))
}

/// Same as [`layout`], but does not check for errors.
///
/// # Safety
///
/// The caller must ensure the error conditions in [`layout`] do not occur.
pub unsafe fn layout_unchecked(size: usize, align: usize) -> Layout {
    unsafe { Layout::from_size_align_unchecked(size, align) }
}
