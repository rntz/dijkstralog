use std::cmp::Ordering;
pub use crate::iter::{Bound, OuterPair};
use Bound::*;

pub trait Seek {
    type Key: Ord + Copy;
    type Value;

    // output bound is >= input bound
    fn seek(&mut self, bound: Bound<Self::Key>) -> Bound<Self::Key>;
    fn ready(&self) -> bool;
    // must only be called if ready(self) holds and the previous seek(self)
    // produced Atleast(key).
    fn value(&self) -> Self::Value;
}

pub struct Iter<S: Seek> {
    pub bound: Bound<S::Key>,
    pub seeker: S,
}
impl<S: Seek> Iterator for Iter<S> {
    type Item = (S::Key, S::Value);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.bound {
                Atleast(k) if self.seeker.ready() => {
                    self.bound = Greater(k); // for next time
                    return Some((k, self.seeker.value()));
                }
                Done => { self.bound = Done; return None; }
                p => { self.bound = p; }
            }
        }
    }
}


// ---------- INNER JOIN ----------
#[derive(Debug, Copy, Clone)]
pub struct Join<X,Y>(X, Y);

impl<X: Seek, Y: Seek<Key = X::Key>> Seek for Join<X,Y> {
    type Key = X::Key;
    type Value = (X::Value, Y::Value);

    fn seek(&mut self, bound: Bound<X::Key>) -> Bound<X::Key>
    { self.1.seek(self.0.seek(bound)) }

    fn ready(&self) -> bool
    { self.0.ready() && self.1.ready() }

    fn value(&self) -> (X::Value, Y::Value)
    { (self.0.value(), self.1.value()) }
}


// ---------- OUTER JOIN ----------
#[derive(Debug, Copy, Clone)]
pub struct OuterJoin<X,Y> {
    left: X,
    right: Y,
    ordering: Ordering,
}

impl<X: Seek, Y: Seek<Key = X::Key>> From<(X,Y)> for OuterJoin<X,Y> {
    // feels like a hack
    fn from(mut value: (X, Y)) -> Self {
        let p = value.0.seek(Init);
        let q = value.1.seek(Init);
        OuterJoin { left: value.0, right: value.1, ordering: p.cmp(&q) }
    }
}

impl<X: Seek, Y:Seek<Key = X::Key>> Seek for OuterJoin<X, Y> {
    type Key = X::Key;
    type Value = OuterPair<X::Value, Y::Value>;

    fn seek(&mut self, bound: Bound<X::Key>) -> Bound<X::Key> {
        let p = self.left.seek(bound);
        let q = self.right.seek(bound);
        self.ordering = p.cmp(&q);
        use Ordering::*;
        match self.ordering { Less | Equal => p, Greater => q }
    }

    // fn seek(&mut self, bound: Bound<X::Key>) -> Bound<X::Key> {
    //     let p = self.left.seek(bound);
    //     let q = self.right.seek(bound);
    //     self.ordering = p.cmp(&q);
    //     use Ordering::*;
    //     match self.ordering { Less | Equal => p, Greater => q }
    // }

    fn ready(&self) -> bool {
        use Ordering::*;
        match self.ordering {
            Less => self.left.ready(),
            Greater => self.right.ready(),
            Equal => self.left.ready() && self.right.ready(),
        }
    }

    fn value(&self) -> OuterPair<X::Value, Y::Value> {
        use Ordering::*;
        match self.ordering {
            Less => OuterPair::Left(self.left.value()),
            Greater => OuterPair::Right(self.right.value()),
            Equal => OuterPair::Both(self.left.value(), self.right.value()),
        }
    }
}
