use std::cmp::Ordering;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum Bound<K> {
    Init,
    Atleast(K),
    Greater(K),
    Done,
}
use Bound::*;

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

    // pub fn outer_join<U>(self: Position<K,V>, other: Position<K,U>) -> Position<K, Outer<V, U>> {
    //     match self.bound().cmp(&other.bound()) {
    //         Ordering::Less    => self.map(|v| Left(v)),
    //         Ordering::Greater => other.map(|v| Right(v)),
    //         Ordering::Equal   => match (self, other) {
    //             (Have(k, x), Have(_, y))    => Have(k, Both(x,y)),
    //             (Know(p), _) | (_, Know(p)) => Know(p),
    //         }
    //     }
    // }
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

    fn bound(&self) -> Bound<Self::Key> { return self.posn().to_bound() }

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

    // fn outer_join<U>(self, other: U) -> OuterJoin<Self, U>
    // where Self: Sized, U: Seek<Key=Self::Key>
    // { OuterJoin(self, other) }

    fn collect<B>(mut self) -> B
    where Self: Sized, B: FromIterator<(Self::Key, Self::Value)>
    { self.iter().collect() }

    fn keys(mut self) -> impl Iterator<Item = Self::Key> where Self: Sized
    { self.iter().map(|(k,v)| k) }

    // fn values(mut self) -> impl Iterator<Item = Self::Value> where Self: Sized
    // { self.iter().map(|(k,v)| v) }

    // fn map_collect<X, F>(mut self, mut func: F) -> Vec<X>
    // where Self: Sized, F: FnMut(Self::Key, Self::Value) -> X
    // { self.iter().map(|(k,v)| func(k,v)).collect() }

    // If the only reason we need the iterator is to look up a particular key,
    // we can do that.
    fn lookup(mut self, key: Self::Key) -> Option<Self::Value> where Self: Sized {
        loop {
            self.seek(Atleast(key));
            return match self.posn() {
                Know(p) if p.matches(key) => continue,
                Have(k, v) if k == key => Some(v),
                _ => None,
            }
        }
    }

    // Fuses a lookup onto the value of an iterator.
    fn map_lookup(self, key: <Self::Value as Seek>::Key) -> impl Seek<Key = Self::Key, Value = <Self::Value as Seek>::Value>
    where Self: Sized, Self::Value: Seek {
        self.filter_map(move |v| v.lookup(key))
    }

    fn iter(self) -> Iter<Self> where Self: Sized { Iter(self) }

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
}

// Rust-native iteration over seekable iterators
pub struct Iter<S: Seek>(pub S);
impl<S: Seek> Iterator for Iter<S> {
    type Item = (S::Key, S::Value);
    fn next(&mut self) -> Option<Self::Item> { self.0.next() }
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


// // ---------- SEEKING IN SORTED LISTS ----------
// #[derive(Clone)]
// struct Slice<'a, K, V> {
//     elems: &'a [(K, V)],
//     index: usize,
// }

// impl<'a, K: Ord, V> Seek for Slice<'a, K, V> {
//     type Key = &'a K;
//     type Value = &'a V;

//     fn posn(&self) -> Position<&'a K, &'a V> {
//         if self.index >= self.elems.len() { return Know(Done) }
//         let (k, v) = &self.elems[self.index];
//         return Have(k, v);
//     }

//     fn seek(&mut self, target: Bound<&'a K>) {
//         self.index += self.elems[self.index..].partition_point(|x| !target.matches(&x.0))
//     }
// }


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
{ Tuples::new(elems, get_key) }

impl<'a, K: Ord + Copy, X, F: Fn(&X) -> K> Tuples<'a, X, F> {
    pub fn new(elems: &'a [X], get_key: F) -> Tuples<'a ,X, F> {
        Tuples { elems, index: 0, get_key }
    }
}

impl<'a, X, K: Ord + Copy, F: Fn(&X) -> K> Seek for Tuples<'a, X, F> {
    type Key = K;
    type Value = &'a X;

    fn posn(&self) -> Position<K, &'a X> {
        if self.index >= self.elems.len() { return Know(Done) }
        let x = &self.elems[self.index];
        return Have((self.get_key)(x), x);
    }

    fn seek(&mut self, target: Bound<K>) {
        self.index += self.elems[self.index..].partition_point(
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

impl<'a, X, F> std::fmt::Debug for Ranges<'a, X, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.debug_struct("Ranges")
            .field("index_lo", &self.index_lo)
            .field("index_hi", &self.index_hi)
            // .field("elems", &self.elems)
            .finish()
    }
}

pub fn ranges<X, K, F>(elems: &[X], get_key: F) -> Ranges<X,F>
where K: Ord + Copy, F: Fn(&X) -> K
{ Ranges::new(elems, get_key) }

impl<'a, X, K: Ord + Copy, F: Fn(&X) -> K> Ranges<'a, X, F> {
    pub fn new(elems: &'a [X], get_key: F) -> Ranges<'a, X, F> {
        let mut s = Ranges { elems, index_lo: 0, index_hi: 0, get_key };
        // NB. This initial seek_hi() might be wasted work if the first
        // operation is a seek() to some higher key. I could avoid it by making
        // posn() first check whether index_lo == index_hi. Is that worth it?
        // Dunno.
        s.seek_hi();
        s
    }

    // adjusts self.index_hi to the end of the region that self.index_lo begins.
    fn seek_hi(&mut self) {
        self.index_hi = self.index_lo + if self.index_lo >= self.elems.len() { 0 } else {
            let key = (self.get_key)(&self.elems[self.index_lo]);
            self.elems[self.index_lo..].partition_point(|x| (self.get_key)(x) == key)
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
        let xs = &self.elems[self.index_lo .. self.index_hi];
        return Have(key, xs);
    }

    fn seek(&mut self, target: Bound<K>) {
        // Optimizations that let us start searching from self.index_hi instead
        // of self.index_lo.
        if self.index_lo >= self.elems.len() { return; }
        if target.matches((self.get_key)(&self.elems[self.index_lo])) { return; }
        self.index_lo = self.index_hi
            + self.elems[self.index_hi..]
                  .partition_point(|x| !target.matches((self.get_key)(x)));
        self.seek_hi();
    }
}


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



// // ---------- OUTER JOIN ----------
// pub struct OuterJoin<X,Y>(pub X, pub Y);

// pub enum Outer<A, B> { Both(A,B), Left(A), Right(B), }
// use Outer::*;

// impl<X: Seek, Y:Seek<Key=X::Key>> Seek for OuterJoin<X,Y> {
//     type Key   = X::Key;
//     type Value = Outer<X::Value, Y::Value>;

//     fn posn(&self) -> Position<X::Key, Outer<X::Value, Y::Value>> {
//         return self.0.posn().outer_join(self.1.posn())
//     }

//     fn seek(&mut self, target: Bound<X::Key>) {
//         self.0.seek(target);
//         self.1.seek(target);
//     }
// }

// // // Outers can also be outer joined.
// // impl<X: Seek, Y:Seek<Key=X::Key>> Seek for Outer<X,Y> {
// //     type Key   = X::Key;
// //     type Value = Outer<X::Value, Y::Value>;

// //     fn posn(&self) -> Position<X::Key, Outer<X::Value, Y::Value>> {
// //         match self {
// //             Left(xs)    => xs.posn().map(|x| Left(x)),
// //             Right(ys)   => ys.posn().map(|y| Right(y)),
// //             Both(xs,ys) => xs.posn().outer_join(ys.posn()),
// //         }
// //     }

// //     fn seek(&mut self, target: Bound<X::Key>) {
// //         match self {
// //             Left(xs)  =>   xs.seek(target),
// //             Right(ys) =>   ys.seek(target),
// //             Both(xs,ys) => { xs.seek(target); ys.seek(target); }
// //         }
// //     }
// // }
