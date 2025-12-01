use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::ptr::null;
use std::ptr::null_mut;

use crate::arena::Arena;
use crate::codec::Decode;
use crate::codec::Decoder;
use crate::codec::Encode;
use crate::codec::Encoder;
use crate::codec::UnsafeDecoder;
use crate::codec::UnsafeEncoder;
use crate::rand::random_u32;
use crate::sync::atomic::AtomicPtr;
use crate::sync::atomic::AtomicUsize;
use crate::sync::atomic::Ordering::AcqRel;
use crate::sync::atomic::Ordering::Acquire;
use crate::sync::atomic::Ordering::Relaxed;

/// The alignment of nodes in the skip list.
pub const ALIGN: usize = align_of::<Node>();

/// A lock-free skip list.
///
/// This skip list is not supposed to be used as it is. It is designed
/// with minimal assumptions about the data it stores.
///
/// This skip list provides unsafe interfaces for upper-level data structures.
/// It assumes that the generic types `K` and `V` used in different methods
/// are compatible with each other.
pub struct SkipList {
    head: Head,
    height: AtomicUsize,
}

impl SkipList {
    /// Creates a new [`SkipList`].
    pub const fn new() -> Self {
        Self {
            head: Head::new(),
            height: AtomicUsize::new(1),
        }
    }

    /// Adds a key-value pair allocated from `arena` to the skip list.
    ///
    /// # Safety
    ///
    /// - `arena` must outlive the skip list.
    /// - `K` and `V` must be compatible with those used in other methods.
    pub unsafe fn add<'a, K, V>(&'a self, k: K, v: V, arena: &Arena<ALIGN>)
    where
        K: Encode + Decode<'a> + Clone + Ord,
        V: Encode,
    {
        let height = self.random_height();
        let node = Node::new(k.clone(), v, height, arena);
        let splice = self.find_splice(&k, height);
        for (level, mut link) in splice.into_iter().enumerate().take(height) {
            loop {
                node.set_next(level, link.next);
                if link.prev.cas_next(level, link.next, node) {
                    break;
                }
                link = self.find_splice_at_level(&k, link.prev, level);
            }
        }
    }

    /// Returns an iterator over the skip list.
    ///
    /// # Safety
    ///
    /// - `K` and `V` must be compatible with those used in other methods.
    pub unsafe fn iter<'a, K, V>(&'a self) -> SkipListIter<'a, K, V>
    where
        K: Decode<'a> + Ord,
        V: Decode<'a>,
    {
        SkipListIter::new(self)
    }
}

/// Private methods.
impl SkipList {
    /// Returns the head node of the skip list.
    fn head(&self) -> &Node {
        self.head.as_node()
    }

    /// Returns the first node in the skip list.
    fn first(&self) -> Option<&Node> {
        self.head().next(0)
    }

    /// Returns the current height of the skip list.
    fn height(&self) -> usize {
        self.height.load(Relaxed)
    }

    /// Returns a random height and updates the skip list height if needed.
    fn random_height(&self) -> usize {
        let height = random_height();
        let mut current = self.height();
        while current < height {
            match self
                .height
                .compare_exchange_weak(current, height, Relaxed, Relaxed)
            {
                Ok(_) => break,
                Err(x) => current = x,
            }
        }
        height
    }

    /// Returns a node <= `k`.
    ///
    /// Returns [`None`] if no such node exists.
    fn seek<'a, K>(&'a self, k: &K) -> Option<&'a Node>
    where
        K: Decode<'a> + Ord,
    {
        let mut prev = self.head();
        for level in (0..self.height()).rev() {
            let link = self.find_splice_at_level(k, prev, level);
            if level == 0 {
                return unsafe { link.next.as_ref() };
            }
            prev = link.prev;
        }
        unreachable!()
    }

    /// Finds a splice to insert a node with `k`.
    fn find_splice<'a, K>(&'a self, k: &K, height: usize) -> [Link<'a>; MAX_HEIGHT]
    where
        K: Decode<'a> + Ord,
    {
        let next = null();
        let mut prev = self.head();
        let mut splice = [Link { prev, next }; MAX_HEIGHT];
        for level in (0..height).rev() {
            let link = self.find_splice_at_level(k, prev, level);
            prev = link.prev;
            splice[level] = link;
        }
        splice
    }

    /// Finds a splice to insert a node with `k` at `level`.
    ///
    /// Returns a [`Link`] with [`Link::prev`] < `k` <= [`Link::next`].
    /// If there is no node >= `k`, [`Link::next`] will be a null pointer.
    fn find_splice_at_level<'a, K>(&'a self, k: &K, start: &'a Node, level: usize) -> Link<'a>
    where
        K: Decode<'a> + Ord,
    {
        let mut prev = start;
        loop {
            match prev.next(level) {
                Some(next) => match next.cmp(k) {
                    Ordering::Less => prev = next,
                    _ => {
                        return Link {
                            prev,
                            next: next as _,
                        };
                    }
                },
                None => {
                    return Link { prev, next: null() };
                }
            }
        }
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

/// An iterator over a [`SkipList`].
#[derive(Clone)]
pub struct SkipListIter<'a, K, V> {
    list: &'a SkipList,
    next: Option<&'a Node>,
    phantom: PhantomData<(K, V)>,
}

impl<'a, K, V> SkipListIter<'a, K, V>
where
    K: Decode<'a> + Ord,
    V: Decode<'a>,
{
    fn new(list: &'a SkipList) -> Self {
        Self {
            list,
            next: list.first(),
            phantom: PhantomData,
        }
    }

    /// Positions the iterator to the first item <= `k`.
    ///
    /// [`Self::next`] will return the specific item, or [`None`] if no such
    /// item exists.
    pub fn seek(&mut self, k: &K) {
        self.next = self.list.seek(k);
    }
}

impl<'a, K, V> Iterator for SkipListIter<'a, K, V>
where
    K: Decode<'a> + Ord,
    V: Decode<'a>,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        self.next.map(|node| {
            self.next = node.next(0);
            node.data()
        })
    }
}

#[derive(Copy, Clone)]
struct Link<'a> {
    prev: &'a Node,
    next: *const Node,
}

/// A special node used as the head of the skip list.
#[repr(transparent)]
struct Head([AtomicPtr<Node>; MAX_HEIGHT]);

impl Head {
    const fn new() -> Self {
        Self([const { AtomicPtr::new(null_mut()) }; MAX_HEIGHT])
    }

    const fn as_node(&self) -> &Node {
        // SAFETY:
        // - `Head` has the same layout as `Node`
        // - The skip list never access data in the head node
        // - The skip list never access more than `MAX_HEIGHT` levels
        unsafe { &*(&self.0[MAX_HEIGHT - 1] as *const _ as *const Node) }
    }
}

/// A node in the skip list.
///
/// Node layout:
///
/// | next[n] | ... | next[1] | next[0] | data |
///
/// A node pointer is always pointing to `next[0]` of the node.
#[repr(transparent)]
struct Node {
    next: [AtomicPtr<Node>; 1],
}

impl Node {
    /// Allocates a node from `arena`.
    fn new<K, V>(k: K, v: V, height: usize, arena: &Arena<ALIGN>) -> &Self
    where
        K: Encode,
        V: Encode,
    {
        let size = size_of::<Node>() * height + k.size() + v.size();
        let node = arena.alloc(size).cast::<Node>();

        // In the std mode, `AtomicPtr` can be initialized later
        // when the node is linked the skip list.
        //
        // In the shuttle mode, `AtomicPtr` is not a simple pointer wrapper,
        // so we need to initialize them here before use.
        #[cfg(feature = "shuttle")]
        for i in 0..height {
            unsafe {
                let ptr = node.add(i).as_ptr() as *mut AtomicPtr<Node>;
                ptr.write(AtomicPtr::new(null_mut()));
            }
        }

        // SAFETY: `node` has enough space for `height` pointers and the data.
        unsafe {
            let ptr = node.add(height - 1);
            let mut enc = UnsafeEncoder::new(ptr.add(1).as_ptr().cast());
            enc.encode(k);
            enc.encode(v);
            ptr.as_ref()
        }
    }

    /// Returns the ordering of this node compared to `other`.
    fn cmp<'de, K>(&self, other: &K) -> Ordering
    where
        K: Decode<'de> + Ord,
    {
        unsafe {
            let mut dec = UnsafeDecoder::new(self.data_ptr().as_ptr());
            let this = dec.decode::<K>();
            this.cmp(other)
        }
    }

    /// Returns the data stored in this node.
    fn data<'de, K, V>(&self) -> (K, V)
    where
        K: Decode<'de>,
        V: Decode<'de>,
    {
        unsafe {
            let mut dec = UnsafeDecoder::new(self.data_ptr().as_ptr());
            let k = dec.decode::<K>();
            let v = dec.decode::<V>();
            (k, v)
        }
    }

    /// Loads the next node at the given level.
    fn next(&self, level: usize) -> Option<&Node> {
        let next = self.next_at(level).load(Acquire);
        unsafe { next.as_ref() }
    }

    /// Stores the next node at the given level.
    fn set_next(&self, level: usize, ptr: *const Node) {
        self.next_at(level).store(ptr.cast_mut(), Relaxed)
    }

    /// Compares and swaps the next node at the given level.
    fn cas_next(&self, level: usize, old: *const Node, new: *const Node) -> bool {
        self.next_at(level)
            .compare_exchange_weak(old.cast_mut(), new.cast_mut(), AcqRel, Relaxed)
            .is_ok()
    }
}

impl Node {
    fn next_at(&self, level: usize) -> &AtomicPtr<Node> {
        unsafe { self.node_ptr().sub(level).as_ref() }
    }

    fn data_ptr(&self) -> NonNull<u8> {
        unsafe { self.node_ptr().add(1).cast() }
    }

    fn node_ptr(&self) -> NonNull<AtomicPtr<Node>> {
        let ptr = &self.next[0] as *const AtomicPtr<Node>;
        unsafe { NonNull::new_unchecked(ptr.cast_mut()) }
    }
}

/// The maximum height of a skip list.
const MAX_HEIGHT: usize = 16;

/// The precomputed result of height possibilities.
const HEIGHT_POSSIBILITIES: [u32; MAX_HEIGHT] = height_possibilities();

/// Returns a random height in `0..MAX_HEIGHT`.
fn random_height() -> usize {
    let mut i = 0;
    let r = random_u32();
    for p in HEIGHT_POSSIBILITIES {
        if r > p {
            break;
        }
        i += 1;
    }
    i
}

/// Precomputes the height possibilities for random height generation.
const fn height_possibilities() -> [u32; MAX_HEIGHT] {
    let mut i = 0;
    let mut p = u32::MAX;
    let mut possibilities = [0; MAX_HEIGHT];
    while i < MAX_HEIGHT {
        possibilities[i] = p;
        i += 1;
        p /= 4;
    }
    possibilities
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::Arena;
    use crate::rand::random_bool;
    use crate::thread;

    struct TestSkipList<K, V> {
        arena: Arena<ALIGN>,
        inner: SkipList,
        phantom: PhantomData<(K, V)>,
    }

    impl<'a, K, V> TestSkipList<K, V>
    where
        K: Encode + Decode<'a> + Clone + Ord,
        V: Encode + Decode<'a>,
    {
        fn new() -> Self {
            Self {
                arena: Arena::new(1024 * 1024),
                inner: SkipList::new(),
                phantom: PhantomData,
            }
        }

        fn add(&'a self, k: K, v: V) {
            unsafe { self.inner.add(k, v, &self.arena) }
        }

        fn iter(&'a self) -> SkipListIter<'a, K, V> {
            unsafe { self.inner.iter() }
        }
    }

    #[test]
    fn test() {
        const N: usize = 10;
        const X: usize = N / 2;

        let list = TestSkipList::new();
        for i in 0..N {
            list.add(i, i);
        }

        // Check iterator next.
        let mut iter = list.iter();
        for i in 0..N {
            assert_eq!(iter.next(), Some((i, i)));
        }
        assert_eq!(iter.next(), None);

        // Check iterator seek.
        iter.seek(&X);
        assert_eq!(iter.next(), Some((X, X)));
        iter.seek(&N);
        assert_eq!(iter.next(), None);
    }

    fn test_concurrent<const N: usize, const T: usize>() {
        let n = AtomicUsize::new(0);
        let list = TestSkipList::new();
        thread::scope(|s| {
            for _ in 0..T {
                s.spawn(|| {
                    loop {
                        let i = n.fetch_add(1, Relaxed);
                        if i >= N {
                            break;
                        }
                        list.add(i, i);
                        list.add(i + N, i + N);
                        if random_bool(0.1) {
                            let mut last = 0;
                            let mut iter = list.iter();
                            while let Some((k, v)) = iter.next() {
                                assert_eq!(k, v);
                                if k > N {
                                    // `k - N` must also exist.
                                    let x = k - N;
                                    iter.seek(&x);
                                    assert_eq!(iter.next(), Some((x, x)));
                                    iter.seek(&k);
                                    assert_eq!(iter.next(), Some((k, k)));
                                }
                                assert!(last <= k, "keys are not in order: {} > {}", last, k);
                                last = k;
                            }
                        }
                    }
                });
            }
        });

        // Check the final state of the skip list.
        let mut iter = list.iter();
        for i in 0..(N * 2) {
            assert_eq!(iter.next(), Some((i, i)));
        }
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_concurrent_std() {
        #[cfg(miri)]
        const N: usize = 1 << 5;
        #[cfg(not(miri))]
        const N: usize = 1 << 10;
        test_concurrent::<N, 4>();
    }

    #[test]
    #[cfg(feature = "shuttle")]
    fn test_concurrent_shuttle() {
        const N: usize = 1 << 7;
        shuttle::check_random(|| test_concurrent::<N, 8>(), 100);
    }
}
