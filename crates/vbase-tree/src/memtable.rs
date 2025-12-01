use std::mem::MaybeUninit;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;
use std::slice;

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
            list: unsafe { bucket.list.as_ref() },
            arena: &self.arena,
        })
    }

    /// Adds a new bucket with the given id.
    pub(crate) fn add_bucket(&self, id: u64) {
        let old = self.load_buckets();
        let new = Bucket {
            id,
            list: self.arena.alloc_value(SkipList::new()),
        };
        let mut buckets = BucketVec::with(self.arena.alloc_slice(old.len() + 1));
        buckets.append(old);
        buckets.push(new);
        buckets.sort();
        self.store_buckets(buckets);
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

#[derive(Clone)]
struct Bucket {
    id: u64,
    list: NonNull<SkipList>,
}

struct BucketVec {
    ptr: NonNull<Bucket>,
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

    fn with(mut ptr: NonNull<[MaybeUninit<Bucket>]>) -> Self {
        let buckets = unsafe { ptr.as_mut() };
        Self {
            ptr: unsafe { NonNull::new_unchecked(buckets.as_mut_ptr().cast()) },
            len: 0,
            cap: buckets.len(),
        }
    }

    fn find(&self, id: u64) -> Option<&Bucket> {
        match self.binary_search_by_key(&id, |b| b.id) {
            Ok(i) => Some(unsafe { self.get_unchecked(i) }),
            Err(_) => None,
        }
    }

    fn sort(&mut self) {
        self.sort_unstable_by_key(|b| b.id);
    }

    fn push(&mut self, bucket: Bucket) {
        assert!(self.cap > self.len);
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
    type Target = [Bucket];

    fn deref(&self) -> &Self::Target {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for BucketVec {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
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

    pub(crate) fn iter(&self) -> MemBucketIter<'a> {
        MemBucketIter {
            iter: unsafe { self.list.iter() },
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

#[cfg(test)]
mod tests {
    use std::iter;

    use vbase_engine::util::rand::random_u64;

    use super::*;

    #[test]
    fn test_memtable() {
        const N: usize = 16;
        const K1: Vid = Vid::new(b"1", 1);
        const K2: Vid = Vid::new(b"2", 2);
        const V1: Value = Value::Value(b"1");
        const V2: Value = Value::Value(b"2");

        let mem = MemTable::new(1024 * 1024);
        let ids = iter::repeat_with(random_u64).take(N).collect::<Vec<_>>();
        let mut buckets = Vec::new();

        // Add buckets
        for &id in &ids {
            assert!(mem.bucket(id).is_none());
            mem.add_bucket(id);
            let bucket = mem.bucket(id).unwrap();
            buckets.push(bucket);
        }

        // Add data to buckets
        for &id in &ids {
            let bucket = mem.bucket(id).unwrap();
            bucket.add(K1, V1);
            bucket.add(K2, V2);
        }

        // The orginal buckets should still be valid.
        for bucket in buckets {
            let mut iter = bucket.iter();
            assert_eq!(iter.next(), Some((K1, V1)));
            assert_eq!(iter.next(), Some((K2, V2)));
            assert_eq!(iter.next(), None);
        }
    }
}
