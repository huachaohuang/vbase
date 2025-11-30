use vbase_util::spmc_queue::Consumer;
use vbase_util::spmc_queue::Producer;
use vbase_util::spmc_queue::Undone;
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
    is_published: AtomicBool,
}

impl Write {
    /// Creates a write with the given LSN.
    fn new(lsn: u64) -> Self {
        Self {
            lsn,
            thread: thread::current(),
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

    /// Marks the write as published.
    fn publish(&self) {
        self.is_published.store(true, Release)
    }

    /// Returns true if the write is published.
    fn is_published(&self) -> bool {
        self.is_published.load(Acquire)
    }
}

impl Default for Write {
    fn default() -> Self {
        Self::new(0)
    }
}

/// A handle to an uncommitted write.
pub(crate) struct WriteHandle<'a>(Undone<'a, Write>);

/// The queue size of the pipeline.
const QUEUE_SIZE: usize = 1 << 12;

/// The submitter side of the pipeline.
pub(crate) struct WriteSubmitter {
    lsn: u64,
    producer: Producer<Write, QUEUE_SIZE>,
}

impl WriteSubmitter {
    /// Submits a write.
    ///
    /// The write should be committed later with [`WriteCommitter::commit`].
    pub(crate) fn submit(&mut self, lsn: u64) -> WriteHandle<'_> {
        let item = self.producer.enqueue(Write::new(lsn));
        WriteHandle(item)
    }

    /// Returns the next LSN.
    pub(crate) fn next_lsn(&mut self) -> u64 {
        self.lsn += 1;
        self.lsn
    }
}

/// The committer side of the pipeline.
pub(crate) struct WriteCommitter {
    lsn: AtomicU64,
    consumer: Consumer<Write, QUEUE_SIZE>,
}

impl WriteCommitter {
    /// Commits the write and publishes its LSN.
    pub(crate) fn commit(&self, handle: WriteHandle) {
        let done = handle.0.done();
        while let Some(item) = self.consumer.dequeue() {
            // If multiple writes are committed and published at the same time,
            // the order of them is not important, because all of them are visible.
            self.publish(item.lsn);
            item.publish();
            item.wake();
        }
        done.wait();
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

/// Creates a pipeline with the last LSN.
pub(crate) fn create_pipeline(lsn: u64) -> (WriteSubmitter, WriteCommitter) {
    let (producer, consumer) = queue();
    let submitter = WriteSubmitter { lsn, producer };
    let committer = WriteCommitter {
        lsn: AtomicU64::new(lsn),
        consumer,
    };
    (submitter, committer)
}
