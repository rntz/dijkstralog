// TODO: rename this module "seek".

use std::cmp::Ordering;
use crate::search::Search;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum Bound<K> {
    Init,
    Atleast(K),
    Greater(K),
    Done,
}
pub use Bound::*;

// this could be extended to handle K: PartialOrd but we don't need it
impl<K: Ord> PartialOrd for Bound<K> {
    fn partial_cmp(&self, other: &Bound<K>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord> Ord for Bound<K> {
    fn cmp(&self, other: &Bound<K>) -> Ordering {
        match (self, other) {
            (Init, Init) | (Done, Done) => Ordering::Equal,
            (Init,    _) | (_   , Done) => Ordering::Less,
            (_   , Init) | (Done,    _) => Ordering::Greater,
            (Atleast(x)  ,  Atleast(y)) |
            (Greater(x)  ,  Greater(y)) => x.cmp(y),
            (Atleast(x)  ,  Greater(y)) => if x <= y { Ordering::Less } else { Ordering::Greater }
            (Greater(x)  ,  Atleast(y)) => if x <  y { Ordering::Less } else { Ordering::Greater }
        }
    }
}

impl<K: Ord> Bound<K> {
    pub fn matches(&self, other: K) -> bool { self <= &Atleast(other) }
}


// ---------- POSITIONS ----------
#[derive(PartialEq, Eq, Debug)]
pub enum Position<K, V> {
    Have(K, V),
    Know(Bound<K>),
}
use Position::*;

impl<K: Ord, V> Position<K, V> {
    pub fn inner_join<U>(self: Position<K,V>, other: Position<K,U>) -> Position<K, (V,U)> {
        match (self, other) {
            (Have(k, x), Have(k2, y)) if k == k2 => Have(k, (x, y)),
            (p, q) => Know(p.to_bound().max(q.to_bound())),
        }
    }
}

impl<K: Ord, V> Position<K,V> {
    pub fn to_bound(self) -> Bound<K> {
        match self {
            Have(k, _) => Atleast(k),
            Know(p)    => p,
        }
    }
}

impl<K: Ord + Copy, V> Position<K, V> {
    pub fn bound(&self) -> Bound<K> {
        match *self {
            Have(k, _) => Atleast(k),
            Know(p)    => p,
        }
    }

    // This is showing up as somewhat costly in samply runs when I use outer
    // joins. Maybe the compiler doesn't know how to optimize it? Disassemble it
    // and examine what's going on.
    pub fn outer_join<U>(self: Position<K,V>, other: Position<K,U>) -> Position<K, OuterPair<V, U>> {
        match self.bound().cmp(&other.bound()) {
            Ordering::Less    => self.map(|v| Left(v)),
            Ordering::Greater => other.map(|v| Right(v)),
            Ordering::Equal   => match (self, other) {
                (Have(k, x), Have(_, y))    => Have(k, Both(x,y)),
                (Know(p), _) | (_, Know(p)) => Know(p),
            }
        }
    }
}

impl<K, V> Position<K, V> {
    pub fn map<U, F: FnOnce(V) -> U>(self, f: F) -> Position<K, U> {
        match self {
            Know(p) => Know(p),
            Have(k, v) => Have(k, f(v)),
        }
    }

    pub fn filter_map<U, F>(self, f: F) -> Position<K, U>
    where F: FnOnce(V) -> Option<U> {
        match self {
            Know(p) => Know(p),
            Have(k, v) => match f(v) {
                Some(u) => Have(k, u),
                None    => Know(Greater(k)),
            }
        }
    }
}

// impl<K: Copy, V> Position<K, V> {
//     #[inline]
//     fn imap<U, F: FnOnce(K, V) -> U>(self, f: F) -> Position<K, U> {
//         match self {
//             Know(p) => Know(p),
//             Have(k, v) => { Have(k, f(k,v)) }
//         }
//     }
// }


// ---------- SEEKABLE ITERATORS ----------
pub trait Seek {
    // Many operations need to copy/clone keys. We could technically get by with
    // only Key: Clone, but for performance it's best if Key: Copy, so we
    // enforce that here.
    type Key: Ord + Copy;
    type Value;

    fn posn(&self) -> Position<Self::Key, Self::Value>;
    fn seek(&mut self, target: Bound<Self::Key>);

    fn bound(&self) -> Bound<Self::Key> { self.posn().to_bound() }

    fn map<V, F>(self, func: F) -> Map<Self, F>
    where Self: Sized, F: Fn(Self::Value) -> V
    { Map { iter: self, func } }

    fn filter_map<V, F>(self, func: F) -> FilterMap<Self, F>
    where Self: Sized, F: Fn(Self::Value) -> Option<V>
    { FilterMap { iter: self, func } }

    // fn imap<V, F>(self, func: F) -> IMap<Self, F>
    // where Self: Sized, F: Fn(Self::Key, Self::Value) -> V
    // { IMap { iter: self, func } }

    fn join<U>(self, other: U) -> Join<Self,U>
    where Self: Sized, U: Seek<Key=Self::Key>
    { Join(self, other) }

    fn outer_join<U>(self, other: U) -> OuterJoin<Self, U>
    where Self: Sized, U: Seek<Key=Self::Key>
    { OuterJoin(self, other) }

    fn collect<B>(self) -> B
    where Self: Sized, B: FromIterator<(Self::Key, Self::Value)>
    { self.iter().collect() }

    fn iter(self) -> Iter<Self> where Self: Sized { Iter(self) }
    fn keys(self) -> IterKeys<Self> where Self: Sized { IterKeys(self) }

    // Like Iterator::next.
    fn next(&mut self) -> Option<(Self::Key, Self::Value)> {
        loop {
            match self.posn() {
                Have(k,v) => {
                    self.seek(Greater(k));
                    return Some((k, v));
                }
                Know(Done) => { return None; }
                Know(p) => self.seek(p),
            }
        }
    }

    // Forcibly advances to a specific key. Will infinite loop if called on an
    // unproductive iterator.
    fn seek_to(&mut self, key: Self::Key) -> Option<Self::Value> {
        loop {
            self.seek(Atleast(key));
            return match self.posn() {
                Know(p) if p.matches(key) => continue,
                Have(k, v) if k == key => Some(v),
                _ => None,
            }
        }
    }

    // Looks up a particular key, consuming the iterator.
    fn lookup(mut self, key: Self::Key) -> Option<Self::Value> where Self: Sized {
        self.seek_to(key)
    }

    // Fuses a lookup onto the value of an iterator.
    fn map_lookup(self, key: <Self::Value as Seek>::Key) -> impl Seek<Key = Self::Key, Value = <Self::Value as Seek>::Value>
    where Self: Sized, Self::Value: Seek {
        self.filter_map(move |v| v.lookup(key))
    }
}

// Rust-native iteration over seekable iterators
pub struct Iter<S: Seek>(pub S);
impl<S: Seek> Iterator for Iter<S> {
    type Item = (S::Key, S::Value);
    fn next(&mut self) -> Option<Self::Item> { self.0.next() }
}

pub struct IterKeys<S: Seek>(pub S);
impl<S: Seek> Iterator for IterKeys<S> {
    type Item = S::Key;
    fn next(&mut self) -> Option<Self::Item> { self.0.next().map(|x| x.0) }
}

pub struct IterValues<S: Seek>(pub S);
impl<S: Seek> Iterator for IterValues<S> {
    type Item = S::Value;
    fn next(&mut self) -> Option<Self::Item> { self.0.next().map(|x| x.1) }
}


// ---------- SEEKING IN AN OPTION ----------
impl<S: Seek> Seek for Option<S> {
    type Key = S::Key;
    type Value = S::Value;

    fn posn(&self) -> Position<S::Key, S::Value> {
        match self {
            None => Know(Done),
            Some(s) => s.posn(),
        }
    }

    fn seek(&mut self, bound: Bound<S::Key>) {
        if let Some(s) = self { s.seek(bound) }
    }
}


// ---------- SORTED DUPLICATE-FREE LIST WITH NO KEY/VALUE STUFF ----------
pub struct Elements<'a, X> {
    elems: &'a [X],
    index: usize,
}

pub fn elements<X: Ord + Copy>(elems: &[X]) -> Elements<X> {
    return Elements { elems, index: 0 }
}

impl<'a, X: Ord + Copy> Seek for Elements<'a, X> {
    type Key = X;
    type Value = ();

    fn posn(&self) -> Position<X, ()> {
        if self.index >= self.elems.len() { Know(Done) }
        else { Have(self.elems[self.index], ()) }
    }

    fn seek(&mut self, target: Bound<X>) {
        self.index += self.elems[self.index..].search(
            |x| !target.matches(*x)
        )
    }
}


// ---------- SEEKING IN A SORTED DUPLICATE-FREE LIST ----------
// Assumes the underlying slice is sorted by get_key() with NO DUPLICATES.
#[derive(Clone)]
pub struct Tuples<'a, X, F> {
    elems: &'a [X],
    index: usize,
    get_key: F,
}

pub fn tuples<K, X, F>(elems: &[X], get_key: F) -> Tuples<X, F>
where K: Ord + Copy, F: Fn(&X) -> K
{ Tuples { elems, index: 0, get_key } }

impl<'a, X, K: Ord + Copy, F: Fn(&X) -> K> Seek for Tuples<'a, X, F> {
    type Key = K;
    type Value = &'a X;

    fn posn(&self) -> Position<K, &'a X> {
        if self.index >= self.elems.len() { return Know(Done) }
        let x = &self.elems[self.index];
        return Have((self.get_key)(x), x);
    }

    fn seek(&mut self, target: Bound<K>) {
        // TAKE 1: search forward from current position
        self.index += self.elems[self.index..].search(
            |x| !target.matches((self.get_key)(x))
        )
    }
}


// ---------- SEEKING IN A SORTED LIST WITH DUPLICATES ----------
// Assumes the underlying slice is sorted by get_key(). Duplicates are allowed;
// produces non-empty sub-slices whose elements all have the same key.
#[derive(Clone)]
pub struct Ranges<'a, X, F> {
    elems: &'a [X],
    index_lo: usize,
    index_hi: usize,
    get_key: F,
}

impl<X, F> std::fmt::Debug for Ranges<'_, X, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Ranges")
            .field("index_lo", &self.index_lo)
            .field("index_hi", &self.index_hi)
            // .field("elems", &self.elems)
            .finish()
    }
}

pub fn ranges<X, K, F>(elems: &[X], get_key: F) -> Ranges<X,F>
where K: Ord + Copy, F: Fn(&X) -> K {
    let mut s = Ranges { elems, index_lo: 0, index_hi: 0, get_key };
    // NB. This initial seek_hi() might be wasted work if the first
    // operation is a seek() to some higher key. I could avoid it by making
    // posn() first check whether index_lo == index_hi. Is that worth it?
    // Dunno.
    s.seek_hi();
    s
}


// TAKE 1: search forward from current position
impl<'a, X, K: Ord + Copy, F: Fn(&X) -> K> Ranges<'a, X, F> {
    // adjusts self.index_hi to the end of the region that self.index_lo begins.
    fn seek_hi(&mut self) {
        self.index_hi = self.index_lo + if self.index_lo >= self.elems.len() { 0 } else {
            let key = (self.get_key)(&self.elems[self.index_lo]);
            self.elems[self.index_lo..].search(|x| (self.get_key)(x) == key)
        };
    }
}

impl<'a, X, K: Ord + Copy, F: Fn(&X) -> K> Seek for Ranges<'a, X, F> {
    type Key = K;
    type Value = &'a [X];

    fn posn(&self) -> Position<K, &'a [X]> {
        if self.index_lo >= self.elems.len() { return Know(Done) }
        debug_assert!(self.index_lo < self.index_hi);
        let key = (self.get_key)(&self.elems[self.index_lo]);
        return Have(key, &self.elems[self.index_lo .. self.index_hi]);
    }

    fn seek(&mut self, target: Bound<K>) {
        let lo = self.index_lo;
        let hi = self.index_hi;
        let len = self.elems.len();
        // Optimizations that let us start searching from self.index_hi instead
        // of self.index_lo.
        if lo >= len { return; }
        if target.matches((self.get_key)(&self.elems[lo])) { return; }
        self.index_lo = hi + self.elems[hi..]
            .search(|x| !target.matches((self.get_key)(x)));
            // .partition_point(|x| !target.matches((self.get_key)(x)));
        self.seek_hi();
    }
}


// // TAKE 2: search from the start every time
// //
// // This seek() calls partition_point() on the WHOLE SEQUENCE every time instead
// // of searching forward from self.index_lo. Bizarrely, for triangle query (where
// // we have 2-level tries and Ranges is used for the outer level only) - this
// // performs BETTER searching forward when Search:search() is partition_point(),
// // and competitively if Search::search is recursive_gallop() or similar.
// //
// // My guesswork explanation: maybe this is friendlier to the branch predictor
// // and possibly also the cache? To the branch predictor, it always does exactly
// // the same number of iterations. To the cache, it basically treats the sorted
// // array like a compressed BST representation; so we're hitting the same nodes
// // over and over again instead of dynamically changing our starting point.
// //
// // But, performance gets MUCH WORSE if I apply this strategy to Tuples as well
// // as Ranges. Wtf?!?! Maybe the better locality of searching forward matters
// // more when we get to individual tuples but when we're looking at big chunks
// // binary search is more competitive?
// //
// // Also I wonder if this still applies once our data is too big for all N levels
// // of the implicit BST to fit in cache. Can that even happen? What about cache
// // collisions?
// //
// // And what if there are more than 2 trie levels?
// impl<'a, X, K: Ord + Copy, F: Fn(&X) -> K> Ranges<'a, X, F> {
//     // adjusts self.index_hi to the end of the region that self.index_lo begins.
//     fn seek_hi(&mut self) {
//         self.index_hi = if self.index_lo >= self.elems.len() {
//             self.index_lo
//         } else {
//             let key = (self.get_key)(&self.elems[self.index_lo]);
//             self.elems.partition_point(|x| (self.get_key)(x) <= key)
//             // self.index_lo + self.elems[self.index_lo..].search(|x| (self.get_key)(x) == key)
//         };
//     }
// }

// impl<'a, X, K: Ord + Copy, F: Fn(&X) -> K> Seek for Ranges<'a, X, F> {
//     type Key = K;
//     type Value = &'a [X];

//     fn posn(&self) -> Position<K, &'a [X]> {
//         if self.index_lo >= self.elems.len() { return Know(Done) }
//         debug_assert!(self.index_lo < self.index_hi);
//         let key = (self.get_key)(&self.elems[self.index_lo]);
//         return Have(key, &self.elems[self.index_lo .. self.index_hi]);
//     }

//     fn seek(&mut self, target: Bound<K>) {
//         if self.index_lo >= self.elems.len() { return; }
//         // if target.matches((self.get_key)(&self.elems[self.index_lo])) { return; }
//         self.index_lo = self.elems.partition_point(|x| !target.matches((self.get_key)(x)));
//         self.seek_hi();
//     }
// }


// ---------- MAP ON VALUES ----------
#[derive(Clone)]
pub struct Map<S, F> { iter: S, func: F }

impl<V, S: Seek, F: Fn(S::Value) -> V> Seek for Map<S, F> {
    type Key = S::Key;
    type Value = V;

    fn posn(&self) -> Position<S::Key, V> {
        self.iter.posn().map(&self.func)
    }

    fn seek(&mut self, target: Bound<S::Key>) {
        self.iter.seek(target)
    }
}


// ---------- FILTER MAP ----------
#[derive(Clone)]
pub struct FilterMap<S, F> { iter: S, func: F }

impl<V, S: Seek, F: Fn(S::Value) -> Option<V>> Seek for FilterMap<S, F> {
    type Key = S::Key;
    type Value = V;
    fn posn(&self) -> Position<S::Key, V> {
        self.iter.posn().filter_map(&self.func)
    }

    fn seek(&mut self, target: Bound<S::Key>) {
        self.iter.seek(target)
    }
}


// // ---------- MAP WITH KEY ----------
// #[derive(Clone)]
// pub struct IMap<S, F> { iter: S, func: F }

// impl<V, S: Seek, F: Fn(S::Key, S::Value) -> V> Seek for IMap<S, F> {
//     type Key = S::Key;
//     type Value = V;

//     fn posn(&self) -> Position<S::Key, V> {
//         self.iter.posn().imap(&self.func)
//     }

//     fn seek(&mut self, target: Bound<S::Key>) {
//         self.iter.seek(target)
//     }
// }


// ---------- FUNCTION QUERIES ----------
pub struct PartialFn<K, F> { bound: Bound<K>, func: F }
pub struct TotalFn<K, F> { bound: Bound<K>, func: F }

impl<K: Ord + Copy, V, F: Fn(K) -> V> TotalFn<K, F> {
    pub fn new(func: F) -> Self { TotalFn { bound: Init, func } }
}
impl<K: Ord + Copy, V, F: Fn(K) -> V> Seek for TotalFn<K, F> {
    type Key = K;
    type Value = V;
    fn seek(&mut self, bound: Bound<K>) { self.bound = bound; }
    fn posn(&self) -> Position<K, V> {
        match self.bound {
            Atleast(k) => Have(k, (self.func)(k)),
            p => Know(p),
        }
    }
}

impl<K: Ord + Copy, V, F: Fn(K) -> Option<V>> PartialFn<K, F> {
    pub fn new(func: F) -> Self { PartialFn { bound: Init, func } }
}
impl<K: Ord + Copy, V, F: Fn(K) -> Option<V>> Seek for PartialFn<K, F> {
    type Key = K;
    type Value = V;
    fn seek(&mut self, bound: Bound<K>) { self.bound = bound; }
    fn posn(&self) -> Position<K, V> {
        match self.bound {
            Atleast(k) => match (self.func)(k) {
                None => Know(Greater(k)),
                Some(v) => Have(k, v),
            },
            p => Know(p),
        }
    }
}


// ---------- RANGE QUERIES ----------
pub fn query_le<K: Ord + Copy>() -> impl Seek<Key = K, Value = impl Seek<Key = K, Value = ()>>
{ TotalFn::new(Atleast) }

pub fn query_eq<K: Ord + Copy>() -> impl Seek<Key = K, Value = impl Seek<Key = K, Value = ()>>
{ TotalFn::new(|k| Singleton::new(k, ())) }

pub struct Singleton<K, V>(Option<(K,V)>);
impl<K, V> Singleton<K, V> {
    pub fn new(k: K, v: V) -> Singleton<K, V> { Singleton(Some((k,v))) }
}

impl<K: Ord + Copy, V: Clone> Seek for Singleton<K, V> {
    type Key = K;
    type Value = V;
    fn posn(&self) -> Position<K, V> {
        match &self.0 {
            None => Know(Done),
            Some((k, v)) => Have(*k, v.clone()),
        }
    }
    fn seek(&mut self, bound: Bound<K>) {
        self.0 = match self.0.take() {
            Some((k,v)) if bound.matches(k) => Some((k,v)),
            _ => None,
        }
    }
}

// Find things satisfying some lower bound.
impl<K: Ord + Copy> Seek for Bound<K> {
    type Key = K;
    type Value = ();
    fn seek(&mut self, bound: Bound<K>) { *self = (*self).max(bound); }
    fn posn(&self) -> Position<K, ()> {
        match self {
            Atleast(k) => Have(*k, ()),
            p => Know(*p),
        }
    }
}


// ---------- INNER JOIN ----------
//
// TODO: n-ary joins using a macro to generate the code. Look at Datafrog for inspo:
// https://github.com/rust-lang/datafrog/blob/07bf407c740db506a56bcb4af3eb474eb83ca815/src/treefrog.rs#L59
pub struct Join<X,Y>(pub X, pub Y);

impl<X: Seek, Y: Seek<Key=X::Key>> Seek for Join<X,Y> {
    type Key   = X::Key;
    type Value = (X::Value, Y::Value);

    fn posn(&self) -> Position<X::Key, (X::Value, Y::Value)> {
        self.0.posn().inner_join(self.1.posn())
    }

    fn seek(&mut self, target: Bound<X::Key>) {
        self.0.seek(target);
        self.1.seek(self.0.bound());
    }
}



// ---------- BINARY OUTER JOIN ----------
#[derive(Debug, Copy, Clone)]
pub struct OuterJoin<X,Y>(pub X, pub Y);

#[derive(Debug, Copy, Clone)]
pub enum OuterPair<A, B> { Both(A,B), Left(A), Right(B), }
use OuterPair::*;

impl<X: Seek, Y:Seek<Key = X::Key>> Seek for OuterJoin<X,Y> {
    type Key   = X::Key;
    type Value = OuterPair<X::Value, Y::Value>;

    fn posn(&self) -> Position<X::Key, OuterPair<X::Value, Y::Value>> {
        return self.0.posn().outer_join(self.1.posn())
    }

    fn seek(&mut self, target: Bound<X::Key>) {
        self.0.seek(target);
        self.1.seek(target);
    }
}

// The results of an outer join can be iterated over as an outer join.
impl<X: Seek, Y:Seek<Key=X::Key>> Seek for OuterPair<X,Y> {
    type Key   = X::Key;
    type Value = OuterPair<X::Value, Y::Value>;

    fn posn(&self) -> Position<X::Key, OuterPair<X::Value, Y::Value>> {
        match self {
            Left(xs)    => xs.posn().map(|x| Left(x)),
            Right(ys)   => ys.posn().map(|y| Right(y)),
            Both(xs,ys) => xs.posn().outer_join(ys.posn()),
        }
    }

    fn seek(&mut self, target: Bound<X::Key>) {
        match self {
            Left(xs)  =>   xs.seek(target),
            Right(ys) =>   ys.seek(target),
            Both(xs,ys) => { xs.seek(target); ys.seek(target); }
        }
    }
}


// ---------- HOMOGENOUS SMALL MULTIWAY OUTER JOIN ----------
use std::mem::MaybeUninit;

#[derive(Debug)] // do we want a Clone instance? we'd have to write it ourselves.
pub struct OuterArray<const N: usize, A> {
    len: usize,
    elems: [MaybeUninit<A>; N],
}

// Hopefully this gets optimized out if A::drop() is a no-op. Otherwise I need
// to re-design this code.
impl<const N: usize, A> Drop for OuterArray<N, A> {
    #[inline(always)]
    fn drop(&mut self) {
        for i in 0..self.len {
            unsafe { self.elems[i].assume_init_drop() }
        }
    }
}

// impl<const N: usize, A> IntoIterator for OuterArray<N, A> {
//     type Item = A;
//     type IntoIter = OuterArrayIntoIter<N, A>;
//     #[inline(always)]
//     fn into_iter(self) -> OuterArrayIntoIter<N, A> {
//         OuterArrayIntoIter {
//             index: 0,
//             len: self.len,
//             elems: self.elems,
//         }
//     }
// }

// pub struct OuterArrayIntoIter<const N: usize, A> {
//     index: usize,
//     len: usize,
//     elems: [MaybeUninit<A>; N],
// }

// impl<const N: usize, A> Iterator for OuterArrayIntoIter<N, A> {
//     type Item = A;
//     fn next(&mut self) -> Option<A> {
//         debug_assert!(self.index <= self.len);
//         if self.index == self.len { return None }
//         let x = unsafe { self.elems[self.index].assume_init_read() };
//         self.index += 1;
//         return Some(x);
//     }
// }

impl<const N: usize, A> OuterArray<N, A> {
    pub const fn new() -> OuterArray<N, A> {
        OuterArray {
            len: 0,
            elems: [ const { MaybeUninit::uninit() }; N],
        }
    }

    pub fn as_slice(&self) -> &[A] {
        // stolen from implementation of [T]::assume_init_ref,
        // https://doc.rust-lang.org/std/primitive.slice.html#method.assume_init_ref
        // https://doc.rust-lang.org/src/core/mem/maybe_uninit.rs.html#1520
        let xs: &[MaybeUninit<A>] = &self.elems[0 .. self.len];
        unsafe { &*(xs as *const [MaybeUninit<A>] as *const [A]) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [A] {
        // stolen from implementation of assume_init_mut,
        // https://doc.rust-lang.org/std/primitive.slice.html#method.assume_init_mut
        let xs: &mut [MaybeUninit<A>] = &mut self.elems[0 .. self.len];
        unsafe { &mut *(xs as *mut [MaybeUninit<A>] as *mut [A]) }
    }

    pub fn push(&mut self, elem: A) {
        if self.len >= N {
            panic!("TOO MANY VALUES TO PUT IN OUTER ARRAY");
        }
        self.elems[self.len].write(elem);
        self.len += 1;
    }

    pub fn singleton(elem: A) -> Self {
        let mut arr = OuterArray::new();
        arr.push(elem);
        return arr
    }
}

impl<const N: usize, A> FromIterator<A> for OuterArray<N, A> {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let mut arr = OuterArray::new();
        for x in iter { arr.push(x) }
        return arr;
    }
}

impl<const N: usize, S: Seek> Seek for OuterArray<N, S> {
    type Key = S::Key;
    type Value = OuterArray<N, S::Value>;

    fn posn(&self) -> Position<S::Key, OuterArray<N, S::Value>> {
        let mut bound: Bound<S::Key> = Done;
        let mut result: Position<S::Key, OuterArray<N, S::Value>> = Know(bound);
        for iter in self.as_slice() {
            let p = iter.posn();
            let b = p.bound();
            match b.cmp(&bound) {
                Ordering::Greater => {} // ignore it.
                Ordering::Equal => match (&mut result, p) {
                    (Have(_, vs), Have(_, v)) => { vs.push(v) }
                    _ => { result = Know(b) }
                }
                Ordering::Less => {
                    bound = b;
                    result = match p {
                        Know(b) => Know(b),
                        Have(k,v) => Have(k, OuterArray::singleton(v)),
                    };
                }
            }
        }
        return result
    }

    fn seek(&mut self, target: Bound<S::Key>) {
        for it in self.as_mut_slice() {
            it.seek(target);
        }
    }
}
