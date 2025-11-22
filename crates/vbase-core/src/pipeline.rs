use vbase_util::arc::Arc;
use vbase_util::spmc_queue::Consumer;
use vbase_util::spmc_queue::Producer;
use vbase_util::spmc_queue::queue;
use vbase_util::sync::atomic::AtomicBool;
use vbase_util::sync::atomic::AtomicU64;
use vbase_util::sync::atomic::Ordering::Acquire;
use vbase_util::sync::atomic::Ordering::Relaxed;
use vbase_util::sync::atomic::Ordering::Release;
use vbase_util::thread;
use vbase_util::thread::Thread;

/// A write in the pipeline.
struct Write {
    lsn: u64,
    thread: Thread,
    is_committed: AtomicBool,
    is_published: AtomicBool,
}

impl Write {
    fn new(lsn: u64) -> Self {
        Self {
            lsn,
            thread: thread::current(),
            is_committed: AtomicBool::new(false),
            is_published: AtomicBool::new(false),
        }
    }

    /// Waits until the write is published.
    fn wait(&self) {
        while !self.is_published() {
            thread::park();
        }
    }

    /// Wakes up the thread waiting for this write.
    fn wake(&self) {
        self.thread.unpark();
    }

    /// Marks the write as committed.
    fn commit(&self) {
        self.is_committed.store(true, Release)
    }

    /// Returns true if the write is committed.
    fn is_committed(&self) -> bool {
        self.is_committed.load(Acquire)
    }

    /// Marks the write as published.
    fn publish(&self) {
        self.is_published.store(true, Release)
    }

    /// Returns true if the write is published.
    fn is_published(&self) -> bool {
        self.is_published.load(Acquire)
    }
}

/// A handle to a write in the pipeline.
pub(crate) struct WriteHandle(Arc<Write>);

impl WriteHandle {
    fn new(lsn: u64) -> Self {
        Self(Write::new(lsn).into())
    }

    pub(crate) fn lsn(&self) -> u64 {
        self.0.lsn
    }
}

/// The queue size of the pipeline.
const QUEUE_SIZE: usize = 1 << 12;

/// The submitter part of the pipeline.
pub(crate) struct WriteSubmitter {
    lsn: u64,
    producer: Producer<Write, QUEUE_SIZE>,
}

impl WriteSubmitter {
    /// Submits a write.
    ///
    /// The write can be committed later with [`WriteCommitter::commit`].
    pub(crate) fn submit(&mut self, lsn: u64) -> WriteHandle {
        let handle = WriteHandle::new(lsn);
        self.producer.enqueue(handle.0.clone());
        handle
    }

    /// Returns the next LSN.
    pub(crate) fn next_lsn(&mut self) -> u64 {
        self.lsn + 1
    }
}

/// The committer part of the pipeline.
pub(crate) struct WriteCommitter {
    lsn: AtomicU64,
    consumer: Consumer<Write, QUEUE_SIZE>,
}

impl WriteCommitter {
    /// Commits the write and publishes its LSN.
    pub(crate) fn commit(&self, handle: WriteHandle) {
        handle.0.commit();
        while let Some(item) = self.consumer.dequeue_if(|x| x.is_committed()) {
            // If multiple writes are committed and published at the same time,
            // the order of publishing them is not important, because all of
            // them are visible to read.
            self.publish(item.lsn);
            item.publish();
            item.wake();
        }
        handle.0.wait();
    }

    /// Publishes `lsn` if it is greater than the current LSN.
    fn publish(&self, lsn: u64) {
        let mut old = self.lsn.load(Relaxed);
        while old < lsn {
            match self.lsn.compare_exchange_weak(old, lsn, Release, Relaxed) {
                Ok(_) => return,
                Err(x) => old = x,
            }
        }
    }

    /// Returns the last published LSN.
    pub(crate) fn last_lsn(&self) -> u64 {
        self.lsn.load(Acquire)
    }
}

/// Creates a pipeline with the given LSN.
pub(crate) fn create_pipeline(lsn: u64) -> (WriteSubmitter, WriteCommitter) {
    let (producer, consumer) = queue();
    let submitter = WriteSubmitter { lsn, producer };
    let committer = WriteCommitter {
        lsn: AtomicU64::new(lsn),
        consumer,
    };
    (submitter, committer)
}
