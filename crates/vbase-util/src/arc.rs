use std::fmt;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::NonNull;

use crate::sync::atomic::AtomicU64;
use crate::sync::atomic::AtomicUsize;
use crate::sync::atomic::Ordering;
use crate::sync::atomic::Ordering::Acquire;
use crate::sync::atomic::Ordering::Relaxed;
use crate::sync::atomic::Ordering::Release;
use crate::sync::atomic::fence;

struct Inner<T: ?Sized> {
    count: AtomicUsize,
    value: T,
}

/// An alternative to [`std::sync::Arc`] that works with [`AtomicArc`].
pub struct Arc<T: ?Sized> {
    ptr: NonNull<Inner<T>>,
    phantom: PhantomData<Inner<T>>,
}

impl<T> Arc<T> {
    pub fn new(value: T) -> Self {
        let inner = Box::new(Inner {
            count: AtomicUsize::new(1),
            value,
        });
        unsafe { Self::from_raw(Box::into_raw(inner)) }
    }
}

impl<T: ?Sized> Arc<T> {
    fn inner(&self) -> &Inner<T> {
        unsafe { self.ptr.as_ref() }
    }

    #[cfg(test)]
    fn count(&self) -> usize {
        self.inner().count.load(Relaxed)
    }

    fn into_raw(self) -> *const Inner<T> {
        let ptr = self.ptr.as_ptr();
        std::mem::forget(self);
        ptr
    }

    unsafe fn from_raw(ptr: *const Inner<T>) -> Self {
        let ptr = unsafe { NonNull::new_unchecked(ptr.cast_mut()) };
        Self {
            ptr,
            phantom: PhantomData,
        }
    }

    unsafe fn increase_count(ptr: *const Inner<T>, count: usize) {
        let inner = unsafe { &*ptr };
        if inner.count.fetch_add(count, Relaxed) > usize::MAX / 2 {
            panic!("reference count overflow");
        }
    }

    unsafe fn decrease_count(ptr: *const Inner<T>, count: usize) {
        let inner = unsafe { &*ptr };
        if inner.count.fetch_sub(count, Release) == count {
            fence(Acquire);
            drop(unsafe { Box::from_raw(ptr.cast_mut()) });
        }
    }
}

unsafe impl<T: ?Sized + Send> Send for Arc<T> {}
unsafe impl<T: ?Sized + Sync> Sync for Arc<T> {}

impl<T: ?Sized> Drop for Arc<T> {
    fn drop(&mut self) {
        unsafe {
            Self::decrease_count(self.ptr.as_ptr(), 1);
        }
    }
}

impl<T: ?Sized> Clone for Arc<T> {
    fn clone(&self) -> Self {
        unsafe {
            Self::increase_count(self.ptr.as_ptr(), 1);
        }
        Self {
            ptr: self.ptr,
            phantom: PhantomData,
        }
    }
}

impl<T: ?Sized> Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner().value
    }
}

impl<T: ?Sized + Eq> Eq for Arc<T> {}

impl<T: ?Sized + PartialEq> PartialEq for Arc<T> {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Arc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(f)
    }
}

impl<T: Default> Default for Arc<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T> From<T> for Arc<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

/// An atomic pointer to [`Arc`] that prevents read-reclaim races.
pub struct AtomicArc<T> {
    state: AtomicU64,
    phantom: PhantomData<Arc<T>>,
}

impl<T> AtomicArc<T> {
    pub fn new(value: Arc<T>) -> Self {
        Self {
            state: AtomicU64::new(new_state(value)),
            phantom: PhantomData,
        }
    }

    pub const fn null() -> Self {
        Self {
            state: AtomicU64::new(0),
            phantom: PhantomData,
        }
    }

    /// Loads the value stored in the pointer.
    ///
    /// Returns `None` if the pointer is null.
    pub fn load(&self, order: Ordering) -> Option<Arc<T>> {
        let state = self.state.fetch_add(REFCOUNT, order);
        let (addr, count) = unpack_state(state);
        if addr == 0 {
            return None;
        }
        if count >= RESERVED_COUNT {
            panic!("external reference count overflow");
        }
        if count >= BACKFILL_COUNT {
            // Backfill the external count to the `Arc`.
            let desired = pack_state(addr);
            let mut current = self.state.load(Relaxed);
            loop {
                let (new_addr, new_count) = unpack_state(current);
                if new_addr != addr || new_count < BACKFILL_COUNT {
                    break;
                }
                match self
                    .state
                    .compare_exchange_weak(current, desired, Relaxed, Relaxed)
                {
                    Ok(_) => unsafe {
                        Arc::<T>::increase_count(addr as _, new_count);
                        break;
                    },
                    Err(x) => current = x,
                }
            }
        }
        Some(unsafe { Arc::from_raw(addr as _) })
    }

    /// Stores `value` into the pointer.
    pub fn store(&self, value: Arc<T>, order: Ordering) {
        let new_state = new_state(value);
        let old_state = self.state.swap(new_state, order);
        drop_state::<T>(old_state);
    }

    /// Clears the pointer to null.
    pub fn clear(&self, order: Ordering) {
        let old_state = self.state.swap(0, order);
        drop_state::<T>(old_state);
    }

    /// Returns true if the pointer is null.
    pub fn is_null(&self, order: Ordering) -> bool {
        let state = self.state.load(order);
        let (addr, _) = unpack_state(state);
        addr == 0
    }
}

impl<T> Drop for AtomicArc<T> {
    fn drop(&mut self) {
        let state = self.state.load(Relaxed);
        drop_state::<T>(state);
    }
}

impl<T> Default for AtomicArc<T> {
    fn default() -> Self {
        Self::null()
    }
}

impl<T> From<Arc<T>> for AtomicArc<T> {
    fn from(value: Arc<T>) -> Self {
        Self::new(value)
    }
}

const REFCOUNT: u64 = 1 << 48;
#[cfg(miri)]
const RESERVED_COUNT: usize = 0x1000;
#[cfg(not(miri))]
const RESERVED_COUNT: usize = 0x8000;
const BACKFILL_COUNT: usize = RESERVED_COUNT / 2;

/// Creates a state from `value`.
///
/// This function acquires the ownership of `value`. The original reference
/// count of `value` will be released in [`drop_state`].
fn new_state<T>(value: Arc<T>) -> u64 {
    let ptr = value.into_raw();
    unsafe {
        Arc::increase_count(ptr, RESERVED_COUNT);
    }
    pack_state(ptr as usize)
}

fn drop_state<T>(state: u64) {
    let (addr, count) = unpack_state(state);
    if addr != 0 {
        unsafe {
            // +1 to release the original reference count.
            Arc::<T>::decrease_count(addr as _, RESERVED_COUNT - count + 1);
        }
    }
}

/// Packs the address into a state.
fn pack_state(addr: usize) -> u64 {
    assert_eq!(addr >> 48, 0);
    addr as u64
}

/// Unpacks the state into (address, external count).
fn unpack_state(state: u64) -> (usize, usize) {
    const MASK: u64 = 0x0000_FFFF_FFFF_FFFF;
    ((state & MASK) as usize, (state >> 48) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_arc() {
        let a = Arc::new(1);
        let b = Arc::new(2);
        let x = AtomicArc::new(a.clone());
        assert_eq!(x.load(Relaxed), Some(a.clone()));
        assert_eq!(a.count(), RESERVED_COUNT + 1);
        x.store(b.clone(), Relaxed);
        assert_eq!(x.load(Relaxed), Some(b.clone()));
        assert_eq!(b.count(), RESERVED_COUNT + 1);
        drop(x);
        assert_eq!(a.count(), 1);
        assert_eq!(b.count(), 1);
    }

    #[test]
    fn test_atomic_arc_null() {
        let a = Arc::new(1);
        let x = AtomicArc::null();
        assert_eq!(x.load(Relaxed), None);
        x.store(a.clone(), Relaxed);
        assert_eq!(x.load(Relaxed), Some(a));
        x.clear(Relaxed);
        assert_eq!(x.load(Relaxed), None);
        assert_eq!(x.is_null(Relaxed), true);
    }

    #[test]
    fn test_atomic_arc_backfill() {
        let a = Arc::new(1);
        let x = AtomicArc::new(a);
        for i in 0..BACKFILL_COUNT {
            let a = x.load(Relaxed).unwrap();
            assert_eq!(a.count(), RESERVED_COUNT + 1 - i);
        }
        {
            // This load will backfill the external count to the `Arc`.
            let a = x.load(Relaxed).unwrap();
            assert_eq!(a.count(), RESERVED_COUNT + 2);
        }
    }
}
