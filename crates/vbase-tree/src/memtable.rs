use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;

use vbase_engine::util::arena::Arena;
use vbase_engine::util::skip_list::ALIGN;
use vbase_engine::util::skip_list::SkipList;
use vbase_engine::util::skip_list::SkipListIter;
use vbase_engine::util::sync::atomic::AtomicPtr;
use vbase_engine::util::sync::atomic::Ordering::Acquire;
use vbase_engine::util::sync::atomic::Ordering::Release;

use crate::data::Value;
use crate::data::Vid;

pub(crate) struct MemTable {
    arena: Arena<ALIGN>,
    buckets: AtomicPtr<BucketVec>,
}

impl MemTable {
    /// Creates a new [`MemTable`] of the given size.
    pub(crate) fn new(size: usize) -> Self {
        let arena = Arena::new(size);
        let buckets = arena.alloc_value(BucketVec::new());
        Self {
            arena,
            buckets: AtomicPtr::new(buckets.as_ptr()),
        }
    }

    /// Gets the bucket with the given id.
    pub(crate) fn bucket(&self, id: u64) -> Option<MemBucket<'_>> {
        let buckets = self.load_buckets();
        buckets.find(id).map(|bucket| MemBucket {
            list: &bucket.list,
            arena: &self.arena,
        })
    }

    /// Adds a new bucket with the given id.
    pub(crate) fn add_bucket(&self, id: u64) {
        let old = self.load_buckets();
        let mut new = BucketVec::with(self.arena.alloc_slice(old.len() + 1));
        new.append(old);
        new.push(self.arena.alloc_value(Bucket::new(id)));
        new.sort();
        self.store_buckets(new);
    }
}

impl MemTable {
    fn load_buckets(&self) -> &BucketVec {
        let ptr = self.buckets.load(Acquire);
        unsafe { &*ptr }
    }

    fn store_buckets(&self, buckets: BucketVec) {
        let ptr = self.arena.alloc_value(buckets);
        self.buckets.store(ptr.as_ptr(), Release);
    }
}

struct Bucket {
    id: u64,
    list: SkipList,
}

impl Bucket {
    fn new(id: u64) -> Self {
        Self {
            id,
            list: SkipList::new(),
        }
    }
}

struct BucketVec {
    ptr: NonNull<NonNull<Bucket>>,
    len: usize,
    cap: usize,
}

impl BucketVec {
    fn new() -> Self {
        Self {
            ptr: NonNull::dangling(),
            len: 0,
            cap: 0,
        }
    }

    fn with(mut ptr: NonNull<[NonNull<Bucket>]>) -> Self {
        let buckets = unsafe { ptr.as_mut() };
        Self {
            ptr: unsafe { NonNull::new_unchecked(buckets.as_mut_ptr()) },
            len: 0,
            cap: buckets.len(),
        }
    }

    fn find(&self, id: u64) -> Option<&Bucket> {
        match self.binary_search_by_key(&id, |b| unsafe { b.as_ref().id }) {
            Ok(i) => Some(unsafe { self.get_unchecked(i).as_ref() }),
            Err(_) => None,
        }
    }

    fn sort(&mut self) {
        self.sort_by_key(|b| unsafe { b.as_ref().id });
    }

    fn push(&mut self, bucket: NonNull<Bucket>) {
        assert!(self.len < self.cap);
        unsafe {
            self.ptr.add(self.len).write(bucket);
            self.len += 1;
        }
    }

    fn append(&mut self, other: &BucketVec) {
        assert!(self.cap - self.len >= other.len);
        unsafe {
            self.ptr
                .add(self.len)
                .copy_from_nonoverlapping(other.ptr, other.len);
            self.len += other.len;
        }
    }
}

impl Deref for BucketVec {
    type Target = [NonNull<Bucket>];

    fn deref(&self) -> &Self::Target {
        unsafe { NonNull::slice_from_raw_parts(self.ptr, self.len).as_ref() }
    }
}

impl DerefMut for BucketVec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { NonNull::slice_from_raw_parts(self.ptr, self.len).as_mut() }
    }
}

pub(crate) struct MemBucket<'a> {
    list: &'a SkipList,
    arena: &'a Arena<ALIGN>,
}

impl<'a> MemBucket<'a> {
    pub(crate) fn add(&self, vid: Vid, value: Value) {
        unsafe {
            self.list.add(vid, value, self.arena);
        }
    }
}

pub(crate) struct MemBucketIter<'a> {
    iter: SkipListIter<'a, Vid<'a>, Value<'a>>,
}

impl<'a> Iterator for MemBucketIter<'a> {
    type Item = (Vid<'a>, Value<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
