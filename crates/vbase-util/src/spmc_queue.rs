use std::mem::ManuallyDrop;
use std::ops::Deref;

use crate::cell::UnsafeCell;
use crate::sync::Arc;
use crate::sync::atomic::AtomicU64;
use crate::sync::atomic::AtomicUsize;
use crate::sync::atomic::Ordering::AcqRel;
use crate::sync::atomic::Ordering::Acquire;
use crate::sync::atomic::Ordering::Relaxed;
use crate::sync::atomic::Ordering::Release;
use crate::thread;

/// Creates a queue of size `N`.
///
/// Returns a producer and a consumer of the queue.
///
/// # Panics
///
/// Panics if `N` is not a power of 2.
pub fn queue<T, const N: usize>() -> (Producer<T, N>, Consumer<T, N>)
where
    T: Send + Sync + Default,
{
    let queue = Arc::new(Queue::new());
    (Producer(queue.clone()), Consumer(queue))
}

const DONE: usize = 1 << (usize::BITS - 1);

/// A slot in the queue.
struct Slot<T> {
    value: UnsafeCell<T>,
    count: AtomicUsize,
}

impl<T> Slot<T> {
    fn new(value: T) -> Self {
        Self {
            value: UnsafeCell::new(value),
            count: AtomicUsize::new(DONE),
        }
    }

    /// Marks the slot as done.
    fn done(&self) {
        self.count.fetch_add(DONE, Release);
    }

    /// Returns true if the slot is done and in use.
    fn is_done(&self) -> bool {
        self.count.load(Acquire) > DONE
    }

    /// Returns true if the slot is done and not in use.
    fn is_free(&self) -> bool {
        self.count.load(Acquire) == DONE
    }

    /// Stores a value into the slot with the given reference count.
    ///
    /// # Safety
    ///
    /// The caller must ensure that no other references to the value exist.
    unsafe fn store(&self, value: T, count: usize) {
        unsafe {
            self.value.set(value);
        }
        self.count.store(count, Release);
    }
}

impl<T: Default> Default for Slot<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

/// A reference to a done item in the queue.
pub struct Done<'a, T>(&'a Slot<T>);

impl<'a, T> Drop for Done<'a, T> {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Release);
    }
}

impl<'a, T> Deref for Done<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: no mutable references exist while the item is in use.
        unsafe { self.0.value.as_ref() }
    }
}

/// A reference to an undone item in the queue.
pub struct Undone<'a, T>(&'a Slot<T>);

impl<'a, T> Undone<'a, T> {
    /// Transitions the item to done state.
    pub fn done(self) -> Done<'a, T> {
        let this = ManuallyDrop::new(self);
        this.0.done();
        Done(this.0)
    }
}

impl<'a, T> Drop for Undone<'a, T> {
    fn drop(&mut self) {
        self.0.done();
        Done(self.0);
    }
}

/// A lock-free, fixed-size, single-producer, multi-consumer queue.
///
/// `N` specifies the size of the queue, which must be a power of 2.
struct Queue<T, const N: usize> {
    /// Packs a 32-bit head index and a 32-bit tail index.
    ///
    /// The head points to the next slot to enqueue.
    /// The tail points to the next slot to dequeue.
    state: AtomicU64,

    /// A fixed-size buffer to store items.
    slots: [Slot<T>; N],
}

impl<T, const N: usize> Queue<T, N>
where
    T: Default,
{
    fn new() -> Self {
        assert!(N.is_power_of_two());
        Self {
            state: AtomicU64::new(0),
            slots: std::array::from_fn(|_| Slot::default()),
        }
    }
}

impl<T, const N: usize> Queue<T, N>
where
    T: Send + Sync,
{
    fn slot(&self, index: usize) -> &Slot<T> {
        &self.slots[index & (N - 1)]
    }
}

fn pack(head: u32, tail: u32) -> u64 {
    (head as u64) << 32 | tail as u64
}

fn unpack(state: u64) -> (u32, u32) {
    ((state >> 32) as u32, state as u32)
}

/// The producer side of a queue.
pub struct Producer<T, const N: usize>(Arc<Queue<T, N>>);

impl<T, const N: usize> Producer<T, N>
where
    T: Send + Sync,
{
    /// Enqueues an item.
    ///
    /// Returns a reference to the enqueued item.
    ///
    /// This function waits for a free slot if the queue is full.
    pub fn enqueue(&mut self, item: T) -> Undone<'_, T> {
        loop {
            let state = self.0.state.load(Acquire);
            let (head, tail) = unpack(state);
            if tail + N as u32 == head {
                // The queue is full
                thread::yield_now();
                continue;
            }

            let slot = self.0.slot(head as usize);
            if !slot.is_free() {
                // The slot hasn't been released
                thread::yield_now();
                continue;
            }

            // SAFETY: the slot is free, so no other references to the value exist.
            unsafe {
                slot.store(item, 2);
            }
            self.0.state.fetch_add(1 << 32, Release);
            return Undone(slot);
        }
    }
}

/// The consumer side of a queue.
#[derive(Clone)]
pub struct Consumer<T, const N: usize>(Arc<Queue<T, N>>);

impl<T, const N: usize> Consumer<T, N>
where
    T: Send + Sync,
{
    /// Dequeues the first item if it is done.
    pub fn dequeue(&self) -> Option<Done<'_, T>> {
        let mut state = self.0.state.load(Acquire);
        loop {
            let (head, tail) = unpack(state);
            if head == tail {
                return None;
            }

            let slot = self.0.slot(tail as usize);
            if !slot.is_done() {
                return None;
            }

            let new_state = pack(head, tail + 1);
            match self
                .0
                .state
                .compare_exchange_weak(state, new_state, AcqRel, Relaxed)
            {
                Ok(_) => return Some(Done(slot)),
                Err(x) => state = x,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::sync::Mutex;
    use crate::sync::atomic::AtomicUsize;
    use crate::sync::atomic::Ordering::Relaxed;

    fn test_concurrent<const N: usize, const T: usize>() {
        let (mut p, c) = queue::<usize, 4>();
        let count = AtomicUsize::new(0);
        let items = Mutex::new(BTreeSet::new());
        thread::scope(|s| {
            s.spawn(|| {
                for i in 0..N {
                    p.enqueue(i);
                }
            });
            for _ in 0..T {
                s.spawn(|| {
                    loop {
                        let i = count.fetch_add(1, Relaxed);
                        if i >= N {
                            break;
                        }
                        loop {
                            if let Some(item) = c.dequeue() {
                                items.lock().unwrap().insert(*item);
                                break;
                            } else {
                                thread::yield_now();
                            }
                        }
                    }
                });
            }
        });

        assert!(c.dequeue().is_none());
        let items = items.lock().unwrap();
        for i in 0..N {
            assert!(items.contains(&i));
        }
    }

    #[test]
    fn test_concurrent_std() {
        #[cfg(miri)]
        const N: usize = 1 << 8;
        #[cfg(not(miri))]
        const N: usize = 1 << 16;
        test_concurrent::<N, 4>();
    }

    #[test]
    #[cfg(feature = "shuttle")]
    fn test_concurrent_shuttle() {
        const N: usize = 1 << 10;
        shuttle::check_random(|| test_concurrent::<N, 8>(), 100);
    }
}
