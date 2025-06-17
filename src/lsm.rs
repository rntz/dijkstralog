// Many things here are adapted from
// https://github.com/frankmcsherry/blog/blob/master/posts/2025-06-03.md

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

// A sorted, de-duplicated list.
// TODO: rename this "Sorted"?
#[derive(Clone)]
pub struct Layer<A> {
    // DataToad uses <Fact as Columnar>::Container here instead of just
    // Vec<Fact>. How important is this?
    elems: Vec<A>,
}

impl<A> Default for Layer<A> { fn default() -> Self { Layer { elems: Vec::new() } } }

impl<A> Layer<A> {
    pub fn as_slice(&self) -> &[A] { self.elems.as_slice() }
    pub fn len(&self) -> usize { self.elems.len() }
}

impl<K, V> Layer<(K, V)>
where
    K: Copy + Ord,
    // In principle we don't need Clone here but it makes implementing merge() a
    // lot easier.
    V: Clone + Add,
{
    pub fn new() -> Self { Layer { elems: Vec::new() } }

    pub fn from_sorted(elems: Vec<(K, V)>) -> Self {
        assert!(elems.is_sorted_by_key(|&(k,_)| k));
        Layer { elems }
    }

    pub fn size(&self) -> usize {
        self.elems.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &(K, V)> {
        self.elems.iter()
    }

    pub fn push(&mut self, key: K, value: V) {
        match self.elems.binary_search_by(|(k,_)| k.cmp(&key)) {
            Err(idx) => self.elems.insert(idx, (key, value)),
            Ok(_) => return,    // already present, do nothing
        }
    }

    pub fn merge(&mut self, other: Layer<(K, V)>) {
        // We should merge two layers only when their sizes are within a power
        // of 2 of each other.
        debug_assert!(self.elems.len() <= 2 * other.elems.len());
        debug_assert!(other.elems.len() <= 2 * self.elems.len());
        debug_assert!(self.elems.is_sorted_by_key(|&(k,_)| k));
        debug_assert!(other.elems.is_sorted_by_key(|&(k,_)| k));

        self.elems = tuples(self.elems.as_slice(), |x| x.0)
            .outer_join(tuples(other.elems.as_slice(), |x| x.0))
            .map(|outer| match outer {
                //  ~*~=~*~  SEMIRING SEMANTICS  ~*~=~*~
                OuterPair::Both((_, x), (_, y)) => x.clone().plus(y.clone()),
                OuterPair::Left((_, x)) => x.clone(),
                OuterPair::Right((_, y)) => y.clone(),
            })
            .iter()
            .collect();

        debug_assert!(self.elems.is_sorted_by_key(|&(k,_)| k));
    }

    // // Below lies an abandoned attempt at an efficient in-place merge. I decided
    // // I'd need to understand more about writing unsafe code safely - for
    // // instance, memory initialization - to implement this.
    // //
    // // see eg https://doc.rust-lang.org/std/mem/union.MaybeUninit.html for some
    // // of the complexity here.
    // //
    // // I think I'd be happy to assume both keys & values are Copy, though, which
    // // should simplify things.
    //
    // pub fn merge(&mut self, mut other: Layer<A>) {
    //     if other.elems.len() > self.elems.len() {
    //         // Always merge smaller into larger.
    //         std::mem::swap(&mut self.elems, &mut other.elems);
    //     }
    //     // Move each element at most once. We use reserve_exact() instead of
    //     // reserve() because we only ever insert into the smallest "nursery"
    //     // layer; once this is merged with a larger layer, a new nursery is
    //     // created.
    //     let n = self.elems.len();
    //     let m = other.elems.len();
    //     self.elems.reserve_exact(m);
    //     debug_assert!(self.elems.capacity() >= n + m);
    //     let _buf = self.elems.as_mut_ptr();
    //     let mut dups = 0usize;
    //     unsafe {
    //         // We merge from the end to avoid overwriting things in self.elems.
    //         // Note that these indexes are all 1-based, so that 0 means "nothing
    //         // left" and we don't need to have negative numbers.
    //         let mut i = n;
    //         let mut j = m;
    //         let mut k = m + n;
    //         while j > 0 {      // while there's stuff to be merged in
    //             debug_assert!(k > 0);
    //             k -= 1;
    //             // put the biggest thing at _buf[k-1].
    //             if i == 0 || *_buf.add(i-1) < other.elems[j] {
    //                 j -= 1;
    //                 *_buf.add(k) = other.elems[j];
    //             } else {
    //                 i -= 1;
    //                 if x > other.elems[j] {
    //                 }
    //             }
    //         }
    //         self.elems.set_len(n + m);
    //     }
    // }
}

// Log-structured merge "tree" of sorted lists.
#[derive(Clone)]
pub struct LSM<A> {
    // A list of layers in order, smallest first.
    // No two layers are within a power of two in size.
    layers: Vec<Layer<A>>,
}

impl<A> Default for LSM<A> { fn default() -> Self { LSM { layers: Vec::new() } } }

impl<'a, A: Copy + Ord> IntoIterator for &'a LSM<A> {
    type Item = A;
    // absurdly large type tiem.
    type IntoIter = IterKeys<OuterArray<{ MAX_LEVELS }, Option<Elements<'a, A>>>>;
    fn into_iter(self) -> Self::IntoIter {
        self.seek(|slice| elements(slice)).keys()
    }
}

impl<A> LSM<A> {
    pub fn layers(&self) -> impl Iterator<Item = &Layer<A>> {
        self.layers.iter()
    }
}

impl<A> LSM<A> where
    A: Copy + Ord,
{
    // We might want to ensure our return type is also Clone; it's helpful to be
    // able to clone Seekerators.
    pub fn seek<'a, S, F>(
        &'a self,                 // <========== IMPORTANT LIFETIME ANNOTATION
        seek_slice: F,
    ) -> OuterArray<{ MAX_LEVELS }, Option<S>> // annoying Option
    where
        S: Seek,
        // Default: needed to construct OuterArray of values in join result tuples.
        S::Value: Default,
        F: Fn(&'a [A]) -> S, // <========== MATCHING LIFETIME ANNOTATION
    {
        assert!(self.layers.len() <= MAX_LEVELS);
        let array: OuterArray<{ MAX_LEVELS }, _> =
            self.layers
            .iter()
            // Some() is an annoying hack to make the iterator type implement
            // Default. TODO: instead create an empty layer and manually
            // initialize the rest of the OuterArray with iterators over it.
            .map(|layer| Some(seek_slice(layer.elems.as_slice())))
            .into();
        return array
    }

}

impl<K, V> LSM<(K, V)> where
    K: Copy + Ord,
    // V: Clone is needed for Layer::merge (currently).
    // V: Default is needed for OuterArray::from.
    V: Clone + Default + Add,
{
    pub fn new() -> Self { LSM { layers: Vec::new() } }

    // TODO: If we hand push() a layer that's bigger than some layers already
    // present, currently it will simply merge all these into itself until it
    // hits a big enough layer. This is... fine? The cost of constructing the
    // layer we're given "pays" (in amortized analysis) for the merges. But it's
    // not necessary; we could instead skip past layers less than half our size.
    // This might be better?
    pub fn push(&mut self, mut layer: Layer<(K, V)>) {
        // Merge layers until factor-of-two invariant is restored.
        while let Some(next) = self.layers.pop_if(|l| l.size() <= 2 * layer.size()) {
            layer.merge(next);
        }
        self.layers.push(layer);
    }

    // Equivalent to self.push(Layer { elems: vec![(key, value)] }) but
    // potentially more efficient. I should probably delete this.
    pub fn push_one(&mut self, key: K, value: V) {
        assert!(self.layers.is_sorted_by_key(|l| std::cmp::Reverse(l.size())));
        match self.layers.pop_if(|n| n.size() < NURSERY_MAX_SIZE) {
            Some(mut nursery) => {
                nursery.push(key, value);
                self.push(nursery);
            }
            // Create a new nursery.
            None => {
                self.layers.push(Layer { elems: vec![(key, value)] });
            }
        };
    }

    // pub fn seek(&self) -> impl Seek<Key=K, Value=V> {
    //     // 4 billion tuples should be enough for anyone!
    //     //
    //     // TODO: bump size to 48 and manually initialize a 48-element OuterArray
    //     // here instead of relying on From.
    //     const N: usize = 32;
    //     assert!(self.layers.len() < N);
    //     let array: OuterArray<N, _> =
    //         self.layers
    //         .iter()
    //     // Some() is an annoying hack to make the iterator type implement
    //     // Default. TODO: instead create an empty layer and manually
    //     // initialize the rest of the OuterArray with iterators over it.
    //         .map(|layer| Some(layer.seek()))
    //         .into();
    //     array.map(|vs| {
    //         // Sum up all the elements we found.
    //         let mut x: V = Add::zero();
    //         for y in vs.as_slice() {
    //             x = x.plus(*y);
    //         }
    //         x
    //     })
    // }

    // 4 billion tuples should be enough for anyone!
    //
    // TODO: bump MAX_LEVELS to 48 and manually initialize a 48-element
    // OuterArray in seek() instead of relying on From.
}
