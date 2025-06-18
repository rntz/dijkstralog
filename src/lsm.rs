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

// NURSERY_MAX_SIZE is the maximum size the smallest LSM layer (the "nursery",
// or layer 0) can be before we make a new one. We may not always reach this max
// size, however, because if the nursery is more than 1/2 the size of the next
// layer up (layer 1), they'll merge and we'll make a new nursery.
//
// For example, suppose NURSERY_MAX_SIZE = 64 and imagine inserting items one by
// one starting from empty. After 64 items we'll promote our first nursery to
// layer 1 and create a new nursery (= layer 0). Once this second nursery has 64
// / 2 = 32 elements, it'll merge with layer 1; now layer 1 has 64 + 32 = 96
// elements. The third nursery grows until it has 96 / 2 = 48 elements, then
// merges into layer 1 to yield 96 + 48 = 144 elements.
//
// Since 144 / 2 = 72 > 64 = NURSERY_MAX_SIZE, once the fourth nursery reaches
// 64 elements we'll promote it, and we'll have:
//
// nursery   0 elements
// layer 1   64 elements
// layer 2   144 elements
//
// And that's how an LSM is grown!
const NURSERY_MAX_SIZE: usize = 64;

// The maximum # of levels we can have in a LSM. We need this because Seek outer
// joins / unions are implemented using fixed-size arrays to avoid heap
// allocation. We need unions to iterate over an LSM because an LSM is the union
// of its layers. Currently this can be at most 32 because we use
// Default::default() to construct our arrays, and Default is only implemented
// for arrays up to size 32 in Rust's stdlib.
pub const MAX_LEVELS: usize = 32;

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Pair<K, V> { pub key: K, pub value: V, }
pub type Key<K> = Pair<K, ()>;

impl<K, V> From<Pair<K, V>> for (K, V) { fn from(pair: Pair<K, V>) -> (K, V) { (pair.key, pair.value) } }
impl<K, V> From<(K, V)> for Pair<K, V> { fn from((key, value): (K,V)) -> Pair<K, V> { Pair { key, value } } }
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

impl<A> Layer<A> {
    pub fn as_slice(&self) -> &[A] { self.elems.as_slice() }
    pub fn len(&self) -> usize { self.elems.len() }
}

impl<K, V> Layer<Pair<K, V>>
where
    K: Copy + Ord,
    // In principle we don't need Clone here but it makes implementing merge() a
    // lot easier.
    V: Clone + Add,
{
    pub fn new() -> Self { Layer { elems: Vec::new() } }

    pub fn from_sorted(elems: Vec<Pair<K, V>>) -> Self {
        assert!(elems.is_sorted_by_key(|x| x.key));
        Layer { elems }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Pair<K, V>> {
        self.elems.iter()
    }

    pub fn push(&mut self, key: K, value: V) {
        match self.elems.binary_search_by(|x| x.key.cmp(&key)) {
            Err(idx) => self.elems.insert(idx, Pair { key, value }),
            Ok(_) => return,    // already present, do nothing
        }
    }

    #[allow(unused_mut)]  // FIXME
    pub fn merge(mut self, other: Layer<Pair<K, V>>) -> Self {
        println!(" merging {} with {}", self.len(), other.len());

        // TODO: Instead of using an outer join, implement this manually. Do I get a
        // significant speedup? Then my outer join implementation sucks and I need to fix
        // it.
        //
        // I get a measurable, but not large, speedup from using this hand-written merge
        // instead of outer_join. Transitive closure on first 150k edges in
        // soc-LiveJournal1.txt goes from 24s (outer_join) to 21s (handwritten).

        if true {              // handwritten
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

        } else {                // outer_join
            self.elems = tuples(self.elems.as_slice(), |x| x.key)
                .outer_join(tuples(other.elems.as_slice(), |x| x.key))
                .map(|outer| match outer {
                    //  ~*~=~*~  SEMIRING SEMANTICS  ~*~=~*~
                    OuterPair::Both(x, y) => x.value.clone().plus(y.value.clone()),
                    OuterPair::Left(x) => x.value.clone(),
                    OuterPair::Right(y) => y.value.clone(),
                })
                .iter()
                .map(|(key, value)| Pair { key, value })
                .collect();
            return self
        }
    }
}

// Log-structured merge "tree" of sorted lists.
#[derive(Clone)]
pub struct LSM<A> {
    // A list of layers in order, smallest first.
    // No two layers are within a power of two in size.
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
    // We might want to ensure our return type is also Clone; it's helpful to be
    // able to clone Seekerators.
    pub fn seek_with<'a, S, F>(
        &'a self,                 // <========== IMPORTANT LIFETIME ANNOTATION
        seek_slice: F,
    ) -> OuterArray<{ MAX_LEVELS }, Option<S>> // annoying Option
    where
        S: Seek,
        F: Fn(&'a [A]) -> S, // <========== MATCHING LIFETIME ANNOTATION
    {
        assert!(self.layers.len() <= MAX_LEVELS);
        let array: OuterArray<{ MAX_LEVELS }, _> =
            self.layers
            .iter()
            // Some() is an annoying hack to make the iterator type implement
            // Default, needed for OuterArray::into(). TODO: instead create an
            // empty layer and manually initialize the rest of the OuterArray
            // with iterators over it.
            .map(|layer| Some(seek_slice(layer.elems.as_slice())))
            .collect();
        return array
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
    // V: Clone + Add needed for Layer::merge. We might be able to get rid of
    // the Clone requirement with some careful programming.
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
                self.layers.push(x.merge(y));
                self.layers.sort_by(|x, y| y.len().cmp(&x.len()));
            }
        }

        println!("Done! result:");
        self.debug_dump("  ");
    }

    // Equivalent to self.push(Layer { elems: vec![(key, value)] }) but
    // potentially more efficient. I should probably delete this.
    pub fn push_one(&mut self, key: K, value: V) {
        match self.layers.pop_if(|n| n.len() < NURSERY_MAX_SIZE) {
            Some(mut nursery) => {
                nursery.push(key, value);
                self.push(nursery);
            }
            // Create a new nursery.
            None => {
                self.layers.push(Layer { elems: vec![Pair { key, value }] });
            }
        };
    }

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
