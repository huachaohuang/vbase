use std::cell;

/// A wrapper around [`cell::UnsafeCell`] to make it more convenient to use.
#[derive(Debug)]
pub struct UnsafeCell<T: ?Sized> {
    inner: cell::UnsafeCell<T>,
}

impl<T> UnsafeCell<T> {
    /// Creates a new [`UnsafeCell`] with the given value.
    pub const fn new(value: T) -> Self {
        Self {
            inner: cell::UnsafeCell::new(value),
        }
    }

    /// Sets the value of the cell.
    ///
    /// # Safety
    ///
    /// It is Undefined Behavior to call this while any reference to the wrapped
    /// value is alive.
    pub unsafe fn set(&self, value: T) {
        unsafe {
            *self.inner.get() = value;
        }
    }
}

impl<T: ?Sized> UnsafeCell<T> {
    /// Gets a reference to the wrapped value.
    ///
    /// # Safety
    ///
    /// It is Undefined Behavior to call this while any mutable reference to the
    /// wrapped value is alive.
    pub unsafe fn as_ref(&self) -> &T {
        unsafe { &*self.inner.get() }
    }

    /// Gets a mutable reference to the wrapped value.
    ///
    /// # Safety
    ///
    /// It is Undefined Behavior to call this while any other reference to the
    /// wrapped value is alive.
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn as_mut(&self) -> &mut T {
        unsafe { &mut *self.inner.get() }
    }
}

unsafe impl<T: ?Sized + Send> Send for UnsafeCell<T> {}
unsafe impl<T: ?Sized + Sync> Sync for UnsafeCell<T> {}

impl<T: Default> Default for UnsafeCell<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}
