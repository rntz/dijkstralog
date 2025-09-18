use crate::iter::*;

// Needed for combining values when deduplicating (eg merging levels).
pub trait Add {
    fn zero() -> Self;
    fn plus(self, other: Self) -> Self;
}

impl Add for () {
    fn zero() -> () { () }
    fn plus(self, _other: ()) -> () { () }
}

// The maximum # of levels we can have in a LSM. We need this because Seek outer joins /
// unions are implemented using fixed-size arrays to avoid heap allocation. We need unions
// to iterate over an LSM because an LSM is the union of its layers. LSM levels grow as
// powers of 2, so 48 levels should be enough for 2^48 - 1 tuples at worst, i.e. the
// addressable memory space on x86-64 and arm64 machines.
pub const MAX_LEVELS: usize = 48;

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Pair<K, V> { pub key: K, pub value: V, }
pub type Key<K> = Pair<K, ()>;

impl<K, V> From<Pair<K, V>> for (K, V) {
    fn from(pair: Pair<K, V>) -> (K, V) { (pair.key, pair.value) }
}
impl<K, V, U: Into<V>> From<(K, U)> for Pair<K, V> {
    fn from((key, value): (K,U)) -> Pair<K, V> { Pair { key, value: value.into() } }
}
impl<K> From<K> for Pair<K, ()> { fn from(key: K) -> Pair<K, ()> { Pair { key, value: () } } }

// A sorted, de-duplicated list.
// TODO: rename this "Sorted"?
#[derive(Clone)]
pub struct Layer<A> {
    // DataToad uses <Fact as Columnar>::Container here instead of just
    // Vec<Fact>. How important is this?
    elems: Vec<A>,
}

impl<A> Default for Layer<A> {
    fn default() -> Self { Layer { elems: Vec::new() } }
}

impl<A> std::ops::Deref for Layer<A> {
    type Target = [A];
    fn deref(&self) -> &[A] { self.elems.as_slice() }
}

impl<A> Layer<A> {
    pub fn new() -> Self { Layer { elems: Vec::new() } }

    pub fn as_slice(&self) -> &[A] { self.elems.as_slice() }

    pub fn iter(&self) -> impl Iterator<Item = &A> {
        self.elems.iter()
    }
}

impl<K: Copy + Ord, V> Layer<Pair<K, V>> {
    pub fn from_sorted(elems: Vec<Pair<K, V>>) -> Self {
        debug_assert!(elems.is_sorted_by_key(|x| x.key));
        Layer { elems }
    }

    pub fn merged(&self, other: &Layer<Pair<K, V>>) -> Self
    where V: Add + Clone
    {
        // this is mostly copied from DataToad I think
        println!(" merging {} with {} nondestructively", self.len(), other.len());
        let mut results: Vec<Pair<K, V>> = Vec::with_capacity(self.elems.len() + other.elems.len());
        let mut iter1 = self.elems.iter().peekable();
        let mut iter2 = other.elems.iter().peekable();
        use std::cmp::Ordering::*;
        while let (Some(x), Some(y)) = (iter1.peek(), iter2.peek()) {
            match x.key.cmp(&y.key) {
                Less => { results.push(iter1.next().unwrap().clone()); }
                Greater => { results.push(iter2.next().unwrap().clone()); }
                Equal => {
                    let mut x = iter1.next().unwrap().clone();
                    let y = iter2.next().unwrap();
                    x.value = x.value.plus(y.value.clone());
                    results.push(x);
                }
            }
        }
        results.extend(iter1.cloned());
        results.extend(iter2.cloned());
        return Layer { elems: results }
    }

    // TODO: a parallel implementation of merge using Rayon. should take argument for
    // number of grains to parallelize across. Recursively split smaller chunk at median,
    // x, and partitioning larger on x. Once they get small enough or we exhaust recursion
    // depth, bail out to in-order merge.
    pub fn merge(self, other: Layer<Pair<K, V>>) -> Self
    where V: Add
    {
        println!(" merging {} with {}", self.len(), other.len());

        // I get a measurable, but not large, speedup from using this hand-written merge
        // instead of merge_outer_join, below. Transitive closure on first 150k edges in
        // soc-LiveJournal1.txt goes from 24s (outer_join) to 21s (handwritten).
        //
        // TODO: Can outer_join be improved to match the performance of this manual
        // implementation?

        // this is mostly copied from DataToad I think
        let mut results: Vec<Pair<K, V>> = Vec::with_capacity(self.elems.len() + other.elems.len());
        let mut iter1 = self.elems.into_iter().peekable();
        let mut iter2 = other.elems.into_iter().peekable();
        use std::cmp::Ordering::*;
        while let (Some(x), Some(y)) = (iter1.peek(), iter2.peek()) {
            match x.key.cmp(&y.key) {
                Less => { results.push(iter1.next().unwrap()); }
                Greater => { results.push(iter2.next().unwrap()); }
                Equal => {
                    let mut x = iter1.next().unwrap();
                    let y = iter2.next().unwrap();
                    x.value = x.value.plus(y.value);
                    results.push(x);
                }
            }
        }
        results.extend(iter1);
        results.extend(iter2);
        return Layer { elems: results }
    }

    // fn merge_outer_join(mut self, other: Layer<Pair<K, V>>) -> Self
    // where V: Add + Clone
    // {
    //     println!(" merging {} with {}", self.len(), other.len());
    //     self.elems = tuples(self.elems.as_slice(), |x| x.key)
    //         .outer_join(tuples(other.elems.as_slice(), |x| x.key))
    //         .map(|outer| match outer {
    //             //  ~*~=~*~  SEMIRING SEMANTICS  ~*~=~*~
    //             OuterPair::Both(x, y) => x.value.clone().plus(y.value.clone()),
    //             OuterPair::Left(x) => x.value.clone(),
    //             OuterPair::Right(y) => y.value.clone(),
    //         })
    //         .iter()
    //         .map(|(key, value)| Pair { key, value })
    //         .collect();
    //     return self
    // }
}

// Log-structured merge "tree" of sorted lists.
//
// TODO: make this cheaply Clone-able. In particular, cloning should not require copying
// any layer's contents; only the vector of layers should need copying. This is because we
// don't mutate layers: we only add and merge them, and merging is not done
// in-place/destructively.
//
// do we have to put Cow<> or RC<> on each layer for this?
pub struct LSM<A> {
    // A list of layers in order, smallest first.
    // No two layers are within a power of two in size.
    //
    // Should we use an OuterArray here? There will be at most 48 layers. But, I doubt
    // this would improve performance measurably.
    layers: Vec<Layer<A>>,
}

impl<A> LSM<A> {
    pub fn new() -> Self { LSM { layers: Vec::new() } }
    pub fn layers(&self) -> impl Iterator<Item = &Layer<A>> { self.layers.iter() }
    pub fn debug_dump(&self, prefix: &str) {
        for (i, l) in self.layers.iter().rev().enumerate() {
            println!("{prefix}{i}: {} entries", l.len());
        }
    }
}

impl<A> Default for LSM<A> { fn default() -> Self { LSM::new() } }

// ===== SEEKERATING an LSM =====
impl<A> LSM<A> {
    pub fn seek_with<'a, S, F>(
        &'a self,               // <========== IMPORTANT LIFETIME ANNOTATION
        seek_slice: F,
    ) -> OuterArray<{ MAX_LEVELS }, S>
    where
        S: Seek,
        F: Fn(&'a [A]) -> S,    // <========== MATCHING LIFETIME ANNOTATION
    {
        assert!(self.layers.len() <= MAX_LEVELS);
        self.layers
            .iter()
            .map(|layer| seek_slice(layer.elems.as_slice()))
            .collect()
    }
}

// impl<'a, A: Copy + Ord> IntoIterator for &'a LSM<A> {
//     type Item = A;
//     // absurdly large type tiem.
//     type IntoIter = IterKeys<OuterArray<{ MAX_LEVELS }, Option<Elements<'a, A>>>>;
//     fn into_iter(self) -> Self::IntoIter {
//         self.seek_with(|slice| elements(slice)).keys()
//     }
// }

// ===== GROWING an LSM =====
// Adapted from the FactLSM::{push, tidy} implementations in
// https://github.com/frankmcsherry/blog/blob/master/posts/2025-06-03.md#data-oriented-design--columns
impl<K, V> LSM<Pair<K, V>> where
    K: Copy + Ord,
    // V: Add used to merge values of the same key on different layers.
    // V: Clone needed to use the non-destructive Layer::merged().
    // By contrast the in-place Layer::merge() doesn't clone.
    V: Clone + Add,
{
    pub fn push(&mut self, layer: Layer<Pair<K, V>>) {
        print!("pushing {} entries into ", layer.len());
        if self.layers.is_empty() {
            println!("empty LSM");
        } else {
            println!("{}-layer LSM:", self.layers.len());
            self.debug_dump("  ");
        }

        self.layers.push(layer);
        self.layers.sort_by(|x, y| y.len().cmp(&x.len())); // sort in descending order
        while let Some(pos) = (1..self.layers.len()).position(
            |i| self.layers[i-1].len() < 2 * self.layers[i].len()
        ) {
            print!("collision at layer {pos},");
            while self.layers.len() > pos + 1 {
                let x = self.layers.pop().unwrap();
                let y = self.layers.pop().unwrap();
                // NB. merge() gives a SMALL speedup over than merged(). trans on 100k edges:
                // 6.3s (merge) vs 6.15s (merged) = 1.025x speedup
                self.layers.push(x.merge(y));
                // self.layers.push(x.merged(&y));
                self.layers.sort_by(|x, y| y.len().cmp(&x.len()));
            }
        }

        println!("Done! result:");
        self.debug_dump("  ");
    }

    // Merges all layers into one. This could probably be more efficient by doing an n-way
    // merge instead of repeated binary merges.
    pub fn into_layer(mut self) -> Layer<Pair<K, V>> {
        let mut layer = match self.layers.pop() {
            None => return Layer::new(),
            Some(x) => x
        };
        while let Some(next) = self.layers.pop() { layer = layer.merge(next); }
        return layer;
    }

    // Compresses LSM down to one layer. Can be useful to speed up future accesses.
    pub fn compress(&mut self) {
        let mut layer = match self.layers.pop() { None => return, Some(x) => x };
        while let Some(next) = self.layers.pop() { layer = layer.merge(next); }
        self.layers.push(layer);
    }
}

impl<K, V> LSM<Pair<K, V>> where K: Copy + Ord, V: Clone + Add {
    pub fn seeker(&self) -> impl Seek<Key = K, Value = V> {
        self.seek_with(|slice| tuples(slice, |x| x.key).map(|x| x.value.clone()))
            .map(|vs| {
                let mut x: V = Add::zero();
                for v in vs.as_slice() {
                    x = x.plus(v.clone());
                }
                x
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = (K, V)> {
        self.seeker().iter()
    }
}
