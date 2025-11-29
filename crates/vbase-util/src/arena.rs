use std::alloc::Layout;
use std::ptr::NonNull;

use bumpalo::Bump;

use crate::alloc::Buffer;
use crate::sync::Mutex;
use crate::sync::atomic::AtomicU64;
use crate::sync::atomic::Ordering::Relaxed;

/// A concurrent arena with a specific alignment.
///
/// The arena preallocates a buffer for fast allocations. When the preallocated
/// buffer is full, it falls back to a bump allocator.
///
/// # Aborts
///
/// Aborts if internal allocation fails because of OOM.
pub struct Arena<const ALIGN: usize = 1> {
    buf: Buffer<ALIGN>,
    offset: AtomicU64,
    fallback: Mutex<Bump>,
}

impl<const ALIGN: usize> Arena<ALIGN> {
    /// Creates an arena with a preallocated buffer of `size`.
    ///
    /// # Panics
    ///
    /// Panics if `size` and `ALIGN` do not form a valid [`Layout`].
    pub fn new(size: usize) -> Self {
        Self {
            buf: Buffer::with_size(size).unwrap(),
            offset: AtomicU64::new(0),
            fallback: Mutex::new(Bump::new()),
        }
    }

    /// Allocates a buffer with at least `size` bytes.
    ///
    /// The returned pointer is always aligned to `ALIGN`.
    ///
    /// # Panics
    ///
    /// Panics if `size` and `ALIGN` do not form a valid [`Layout`].
    pub fn alloc(&self, size: usize) -> NonNull<u8> {
        // Cast to `u64` to avoid overflow.
        let size64 = size as u64;
        let size64 = size64.next_multiple_of(ALIGN as u64);
        let offset = self.offset.fetch_add(size64, Relaxed);
        if offset + size64 <= self.buf.size() as u64 {
            unsafe {
                let ptr = self.buf.as_ptr().add(offset as usize);
                return NonNull::new_unchecked(ptr.cast_mut());
            }
        }
        let layout = Layout::from_size_align(size, ALIGN).unwrap();
        self.fallback.lock().unwrap().alloc_layout(layout)
    }

    /// Allocates a value and initializes it with `value`.
    ///
    /// # Panics
    ///
    /// Panics if the following conditions are not met:
    ///
    /// - `ALIGN` must be a multiple of the alignment of `T`.
    /// - The size of `value` and `ALIGN` must form a valid [`Layout`].
    pub fn alloc_value<T>(&self, value: T) -> NonNull<T> {
        assert!(ALIGN.is_multiple_of(align_of::<T>()));
        let ptr = self.alloc(size_of::<T>()).cast::<T>();
        unsafe {
            ptr.write(value);
        }
        ptr
    }

    /// Allocates a slice of `len` elements.
    ///
    /// Returns a pointer to an uninitialized slice.
    ///
    /// # Panics
    ///
    /// Panics if the following conditions are not met:
    ///
    /// - `ALIGN` must be a multiple of the alignment of `T`.
    /// - The total size of the slice and `ALIGN` must form a valid [`Layout`].
    pub fn alloc_slice<T>(&self, len: usize) -> NonNull<[T]> {
        assert!(ALIGN.is_multiple_of(align_of::<T>()));
        let Some(size) = size_of::<T>().checked_mul(len) else {
            panic!(
                "allocate {len} elements of size {} overflows",
                size_of::<T>()
            );
        };
        NonNull::slice_from_raw_parts(self.alloc(size).cast(), len)
    }

    /// Returns the approximate number of bytes allocated from the arena.
    pub fn allocated_size(&self) -> usize {
        self.offset.load(Relaxed).try_into().unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        const SIZE: usize = 1024;
        const ALIGN: usize = 8;
        let arena = Arena::<ALIGN>::new(SIZE);

        // Actual allocation size should be 8.
        let ptr = arena.alloc(1);
        assert_eq!(ptr.align_offset(ALIGN), 0);
        assert_eq!(arena.allocated_size(), 8);

        // Actual allocation size should be 56.
        let ptr = arena.alloc(50);
        assert_eq!(ptr.align_offset(ALIGN), 0);
        assert_eq!(arena.allocated_size(), 64);

        // Fall back allocation.
        let ptr = arena.alloc(SIZE);
        assert_eq!(ptr.align_offset(ALIGN), 0);
        assert_eq!(arena.allocated_size(), SIZE + 64);

        // Allocate values.
        let ptr = arena.alloc_value(42u32);
        assert_eq!(unsafe { ptr.as_ref() }, &42u32);
        let ptr = arena.alloc_value(42u64);
        assert_eq!(unsafe { ptr.as_ref() }, &42u64);

        // Allocate slices.
        let ptr = arena.alloc_slice::<u32>(0);
        assert_eq!(unsafe { ptr.as_ref() }, &[]);
        let ptr = arena.alloc_slice::<u64>(8);
        assert_eq!(unsafe { ptr.as_ref().len() }, 8);
    }

    #[test]
    fn test_zst() {
        const ALIGN: usize = 8;
        let arena = Arena::<ALIGN>::new(0);

        let ptr = arena.alloc(0);
        assert_eq!(ptr.align_offset(ALIGN), 0);
        let ptr = arena.alloc_value(());
        assert_eq!(ptr.align_offset(ALIGN), 0);
        let ptr = arena.alloc_slice::<()>(8);
        assert_eq!(ptr.as_ptr().addr() % ALIGN, 0);
    }
}
