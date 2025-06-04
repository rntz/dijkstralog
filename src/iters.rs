use std::cmp::Ordering;
// use core::ops::{Add,Mul};
use std::fmt;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum Bound<K> { Init, Atleast(K), Greater(K), Done }
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

#[derive(PartialEq, Eq, Debug)]
pub enum Position<K, V> { Have(K, V), Know(Bound<K>) }
use Position::*;

impl<K: Ord, V> Position<K, V> {
    pub fn inner_join<U>(self: Position<K,V>, other: Position<K,U>) -> Position<K, (V,U)> {
        match (self, other) {
            (Have(k, x), Have(k2, y)) if k == k2 => Have(k, (x, y)),
            (p, q) => Know(p.to_bound().max(q.to_bound())),
        }
    }
}

impl<K: Ord + Clone, V> Position<K, V> {
    pub fn outer_join<U>(self: Position<K,V>, other: Position<K,U>) -> Position<K, Outer<V, U>> {
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

impl<K: Clone, V> Position<K, V> {
    pub fn bound(&self) -> Bound<K> {
        match self {
            Have(k, _) => Atleast(k.clone()),
            Know(p)    => p.clone(),
        }
    }
}

impl<K, V> Position<K, V> {
    pub fn to_bound(self) -> Bound<K> {
        match self {
            Have(k, _) => Atleast(k),
            Know(p)    => p,
        }
    }

    pub fn map<U, F>(self, f: F) -> Position<K, U>
    where F: FnOnce(V) -> U {
        match self {
            Know(p) => Know(p),
            Have(k, v) => Have(k, f(v)),
        }
    }

    // fn map_with_key<U, F>(self, f: F) -> Position<K, U>
    // where F: FnOnce(&K,V) -> U {
    //     match self {
    //         Know(p) => Know(p),
    //         Have(k, v) => { let u = f(&k,v); Have(k, u) }
    //     }
    // }

    pub fn filter_map<U, F>(self, f: F) -> Position<K, U>
    where F : FnOnce(&K, V) -> Option<U> {
        match self {
            Know(p) => Know(p),
            Have(k, v) => match f(&k, v) {
                Some(u) => Have(k, u),
                None    => Know(Greater(k)),
            }
        }
    }
}

pub trait Seek {
    // Many operations need to copy/clone keys. We could technically get by with
    // only Key: Clone, but for performance it's best if Key: Copy, so we
    // enforce that here.
    type Key: Ord + Copy;
    type Value;

    fn posn(&self) -> Position<Self::Key, Self::Value>;
    fn seek(&mut self, target: Bound<Self::Key>);

    fn bound(&self) -> Bound<Self::Key> { return self.posn().to_bound() }

    fn map<B,F>(self, func: F) -> Map<Self,F>
    where Self: Sized, F: Fn(Self::Value) -> B
    { Map { iter: self, func } }

    // fn map_with_key<B,F>(self, func: F) -> Map<Self,F>
    // where Self: Sized, F: Fn(Self::Key, Self::Value) -> B
    // { MapWithKey { iter: self, func } }

    fn join<U>(self, other: U) -> Join<Self,U>
    where Self: Sized, U: Seek<Key=Self::Key>
    { Join(self, other) }

    fn outer_join<U>(self, other: U) -> OuterJoin<Self, U>
    where Self: Sized, U: Seek<Key=Self::Key>
    { OuterJoin(self, other) }

    fn collect_with<X, F>(mut self, mut func: F) -> Vec<X>
    where Self: Sized,
          F: FnMut(Self::Key, Self::Value) -> X
    {
        let mut xs = Vec::new();
        loop {
            match self.posn() {
                Have(k,v) => {
                    xs.push(func(k,v));
                    self.seek(Greater(k));
                }
                Know(Done) => break,
                Know(p) => self.seek(p),
            }
        }
        return xs;
    }

    fn collect(mut self) -> Vec<(Self::Key, Self::Value)> where Self: Sized {
        self.collect_with(|k,v| (k, v))
    }
}


// Rust-native iteration over seekable iterators
pub struct Iter<S: Seek>(S);

impl<S: Seek> Iterator for Iter<S> {
    type Item = (S::Key, S::Value);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.0.posn() {
                Have(k,v) => {
                    self.0.seek(Greater(k));
                    return Some((k, v));
                }
                Know(Done) => { return None; }
                Know(p) => self.0.seek(p),
            }
        }
    }
}


// pub trait Additive<Rhs> {
//     type Output;
//     fn plus(self, other: Rhs) -> Self::Output;
// }

// // THIS IS IMPOSSIBLE, BECAUSE WE NEED DYNAMIC DISPATCH!
// // imagine nested addition.
// // if we get a left iterator but not a right iterator, we want to just return it.
// // but if we get both left & right we need to outerjoin them.
// // this means that the type of our iterators (compile time)
// // will depend on the structure of our data (run time).
// // oh no!
// //
// // I guess the solution is an enum with three branches?
// // ugh. this feels dumb.
// // no, it has to have arbitrarily many branches. hmmmm.
// // this requires memory allocation. blech. I hate it.
// impl<X: Seek, Y: Seek<Key = X::Key>> Additive<Y> for X
// where
//     X::Value: core::ops::Add<Y::Value>,
// {
//     //type Output = <X::Value as core::ops::Add>::Output;
//     type Output = Map<OuterJoin<X,Y>, _>; // how do I even name this type?
//     // looks like I'll have to create a custom struct just for this, ugh.
//     fn plus(self, other: Y) -> Self::Output {
//         self.outer_join(other).map(|_,x| match x {
//             Outer::Both(x,y) => todo!(),
//             Outer::Left(x) => todo!(), // IMPOSSIBLE
//             Outer::Right(y) => todo!(),
//         })
//     }
// }



// ---------- SEEKING IN SORTED LISTS ----------
#[derive(Clone)]
struct Slice<'a, K, V> {
    elems: &'a [(K, V)],
    index: usize,
}

impl<'a, K: Ord, V> Seek for Slice<'a, K, V> {
    type Key = &'a K;
    type Value = &'a V;

    fn posn(&self) -> Position<&'a K, &'a V> {
        if self.index >= self.elems.len() { return Know(Done) }
        let (k, v) = &self.elems[self.index];
        return Have(k, v);
    }

    fn seek(&mut self, target: Bound<&'a K>) {
        self.index += self.elems[self.index..].partition_point(|x| !target.matches(&x.0))
    }
}


// ---------- SEEKING BY A FUNCTION IN A SORTED LIST ----------
// Assumes the underlying slice is sorted by get_key() with NO DUPLICATES.
#[derive(Clone)]
pub struct SliceBy<'a, X, F> {
    elems: &'a [X],
    index: usize,
    get_key: F,
}

impl<'a, K: Ord + Clone, X, F: Fn(&X) -> K> SliceBy<'a, X, F> {
    pub fn new(elems: &'a [X], get_key: F) -> SliceBy<'a ,X, F> {
        SliceBy { elems, index: 0, get_key }
    }
}

impl<'a, X, K: Ord + Copy, F: Fn(&X) -> K> Seek for SliceBy<'a, X, F> {
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


// ---------- SEEKING RANGES IN SORTED LISTS ----------
// Assumes the underlying slice is sorted by get_key(). Duplicates are allowed;
// produces non-empty sub-slices whose elements all have the same key.
#[derive(Clone)]
pub struct SliceRange<'a, X, F> {
    elems: &'a [X],
    index_lo: usize,
    index_hi: usize,
    get_key: F,
}

impl<'a, X, F> fmt::Debug for SliceRange<'a, X, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.debug_struct("SliceRange")
            .field("index_lo", &self.index_lo)
            .field("index_hi", &self.index_hi)
            // .field("elems", &self.elems)
            .finish()
    }
}

impl<'a, X, K: Ord + Clone, F: Fn(&X) -> K> SliceRange<'a, X, F> {
    pub fn new(elems: &'a [X], get_key: F) -> SliceRange<'a, X, F> {
        let mut s = SliceRange { elems, index_lo: 0, index_hi: 0, get_key };
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

impl<'a, X, K: Ord + Copy, F: Fn(&X) -> K> Seek for SliceRange<'a, X, F> {
    type Key = K;
    type Value = &'a [X];

    fn posn(&self) -> Position<K, &'a [X]> {
        if self.index_lo >= self.elems.len() { return Know(Done) }
        let key = (self.get_key)(&self.elems[self.index_lo]);
        let xs = &self.elems[self.index_lo .. self.index_hi];
        return Have(key, xs);
    }

    fn seek(&mut self, target: Bound<K>) {
        self.index_lo = self.index_hi + self.elems[self.index_hi..].partition_point(
            |x| !target.matches((self.get_key)(x))
        );
        self.seek_hi()
    }
}


// ---------- MAP ----------
#[derive(Clone)]
pub struct Map<Iter, F> { iter: Iter, func: F }

impl<B, Iter, F> Seek for Map<Iter,F> where
    Iter: Seek,
    F: Fn(Iter::Value) -> B
{
    type Key = Iter::Key;
    type Value = B;

    fn posn(&self) -> Position<Iter::Key, B> {
        self.iter.posn().map(&self.func)
    }

    fn seek(&mut self, target: Bound<Iter::Key>) {
        self.iter.seek(target)
    }
}


// // ---------- MAP WITH KEY ----------
// pub struct MapWithKey<Iter, F> { iter: Iter, func: F }

// impl<B, Iter, F> Seek for MapWithKey<Iter,F> where
//     Iter: Seek,
//     F: Fn(&Iter::Key, Iter::Value) -> B
// {
//     type Key = Iter::Key;
//     type Value = B;

//     fn posn(&self) -> Position<Iter::Key, B> {
//         self.iter.posn().map_with_key(&self.func)
//     }

//     fn seek(&mut self, target: &Bound<Iter::Key>) {
//         self.iter.seek(target)
//     }
// }


// ---------- INNER JOIN ----------
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

impl<X: Seek, Y: Seek<Key=X::Key>> Seek for (X,Y) {
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



// ---------- OUTER JOIN ----------
pub struct OuterJoin<X,Y>(pub X, pub Y);

pub enum Outer<A, B> { Both(A,B), Left(A), Right(B), }
use Outer::*;

impl<X: Seek, Y:Seek<Key=X::Key>> Seek for OuterJoin<X,Y> {
    type Key   = X::Key;
    type Value = Outer<X::Value, Y::Value>;

    fn posn(&self) -> Position<X::Key, Outer<X::Value, Y::Value>> {
        return self.0.posn().outer_join(self.1.posn())
    }

    fn seek(&mut self, target: Bound<X::Key>) {
        self.0.seek(target);
        self.1.seek(target);
    }
}

// Outers can also be outer joined.
impl<X: Seek, Y:Seek<Key=X::Key>> Seek for Outer<X,Y> {
    type Key   = X::Key;
    type Value = Outer<X::Value, Y::Value>;

    fn posn(&self) -> Position<X::Key, Outer<X::Value, Y::Value>> {
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


// // ---------- TRIES?? ----------
// trait Unsplit { type First; type Rest; fn unsplit(k: Self) -> (Self::First, Self::Rest); }

// impl<X> Unsplit for (X) {
//     type First = X; type Rest = ();
//     fn unsplit(x: (X)) -> (X,()) { (x.0, ()) }
// }


// // Addition.
// pub enum Adder<A,B> { Both(OuterJoin<A,B>), Left(A), Right(B) }

// // CONFLICTING IMPLEMENTATIONS ARGH
// impl<A,B> Seek for Adder<A,B>
// where
//     A: Seek,
//     B: Seek<Key = A::Key, Value = A::Value>,
//     A::Value: core::ops::Add<A::Value, Output = A::Value>
// {
//     type Key = A::Key;
//     type Value = A::Value;
// }

// impl<A,B> Seek for Adder<A,B>
// where
//     A: Seek,
//     B: Seek<Key = A::Key>,
//     A::Value: Seek,
//     B::Value: Seek,
// {
//     type Key = A::Key;
//     type Value = Adder<A::Value, B::Value>;

//     fn posn(&self) -> Position<Self::Key, Self::Value> {
//         match self {
//             Adder::Left(x) => x.posn().map(|v| Adder::Left(v)),
//             Adder::Right(y) => y.posn().map(|v| Adder::Right(v)),
//             Adder::Both(xy) => xy.posn().map(|v| match v {
//                 Left(a)   => Adder::Left(a),
//                 Right(b)  => Adder::Right(b),
//                 Both(a,b) => Adder::Both(OuterJoin(a,b)),
//             })
//         }
//     }

//     fn seek(&mut self, bound: &Bound<Self::Key>) {
//         match self {
//             Adder::Left(x)  => x.seek(bound),
//             Adder::Right(y) => y.seek(bound),
//             Adder::Both(xy) => xy.seek(bound),
//         }
//     }
// }


// fn add<X, Y>(xs: X, ys: Y) -> impl Seek
// where
//     X: Seek,
//     Y: Seek<Key=X::Key, Value=X::Value>,
//     X::Key: Clone,
//     X::Value: core::ops::Add<Output = X::Value>,
// {
//     OuterJoin(xs, ys).map(|thing| match thing {
//         Both(x,y) => x + y,
//         Left(x) => x,
//         Right(y) => y,
//     })
// }
