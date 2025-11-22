use crate::arc::Arc;
use crate::arc::AtomicArc;
use crate::sync::atomic::AtomicU64;
use crate::sync::atomic::Ordering::Acquire;
use crate::sync::atomic::Ordering::Relaxed;
use crate::sync::atomic::Ordering::Release;
use crate::thread;

/// Creates a queue of size `N`.
///
/// Returns the producer/consumer of the queue.
///
/// # Panics
///
/// Panics if `N` is not a power of 2.
pub fn queue<T: Send + Sync, const N: usize>() -> (Producer<T, N>, Consumer<T, N>) {
    let queue = Arc::new(Queue::new());
    (Producer(queue.clone()), Consumer(queue))
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
    slots: [AtomicArc<T>; N],
}

impl<T: Send + Sync, const N: usize> Queue<T, N> {
    fn new() -> Self {
        assert!(N.is_power_of_two());
        Self {
            state: AtomicU64::new(0),
            slots: [const { AtomicArc::null() }; N],
        }
    }

    fn slot(&self, index: usize) -> &AtomicArc<T> {
        &self.slots[index & (N - 1)]
    }
}

fn pack(head: u32, tail: u32) -> u64 {
    (head as u64) << 32 | tail as u64
}

fn unpack(state: u64) -> (u32, u32) {
    ((state >> 32) as u32, state as u32)
}

/// The producer part of a queue.
pub struct Producer<T, const N: usize>(Arc<Queue<T, N>>);

impl<T: Send + Sync, const N: usize> Producer<T, N> {
    /// Enqueues an item.
    ///
    /// If the queue is full, this function waits until the queue is not full.
    pub fn enqueue(&self, item: Arc<T>) {
        loop {
            let state = self.0.state.load(Acquire);
            let (head, tail) = unpack(state);
            if tail + N as u32 == head {
                // The queue is full
                thread::yield_now();
                continue;
            }

            let slot = self.0.slot(head as usize);
            if !slot.is_null(Acquire) {
                // The slot hasn't been released
                thread::yield_now();
                continue;
            }

            slot.store(item, Release);
            self.0.state.fetch_add(1 << 32, Release);
            return;
        }
    }
}

/// The consumer part of a queue.
#[derive(Clone)]
pub struct Consumer<T, const N: usize>(Arc<Queue<T, N>>);

impl<T: Send + Sync, const N: usize> Consumer<T, N> {
    /// Dequeues the first item.
    pub fn dequeue(&self) -> Option<Arc<T>> {
        self.dequeue_if(|_| true)
    }

    /// Dequeues the first item if `pred` returns true on the item.
    pub fn dequeue_if<F>(&self, pred: F) -> Option<Arc<T>>
    where
        F: Fn(&T) -> bool,
    {
        let mut state = self.0.state.load(Acquire);
        loop {
            let (head, tail) = unpack(state);
            if head == tail {
                return None;
            }

            let slot = self.0.slot(tail as usize);
            let item = slot.load(Acquire);
            match item.as_ref() {
                Some(x) if pred(x) => {}
                _ => return None,
            }

            let new_state = pack(head, tail + 1);
            match self
                .0
                .state
                .compare_exchange_weak(state, new_state, Release, Relaxed)
            {
                Ok(_) => {
                    slot.clear(Release);
                    return item;
                }
                Err(x) => state = x,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_concurrent(num_ops: u64) {
        let (p, c) = queue::<u64, 4>();
        let producer = thread::spawn(move || {
            for i in 0..num_ops {
                p.enqueue(i.into());
            }
        });

        let mut consumers = Vec::new();
        let n = Arc::new(AtomicU64::new(0));
        for _ in 0..4 {
            let c = c.clone();
            let n = n.clone();
            consumers.push(thread::spawn(move || {
                loop {
                    let i = n.fetch_add(1, Relaxed);
                    if i >= num_ops {
                        break;
                    }
                    loop {
                        if let Some(item) = c.dequeue_if(|x| *x == i) {
                            assert_eq!(*item, i);
                            break;
                        } else {
                            thread::yield_now();
                        }
                    }
                }
            }))
        }

        producer.join().unwrap();
        consumers.into_iter().for_each(|c| c.join().unwrap());
        assert_eq!(c.dequeue(), None);
    }

    #[test]
    fn test_spmc_queue() {
        #[cfg(miri)]
        const N: u64 = 1 << 8;
        #[cfg(not(miri))]
        const N: u64 = 1 << 16;
        test_concurrent(N);
    }

    #[test]
    #[cfg(feature = "shuttle")]
    fn test_spmc_queue_shuttle() {
        const N: u64 = 1 << 8;
        shuttle::check_random(|| test_concurrent(N), 100);
    }
}
