#![allow(unused_imports)]
use std::cmp::Ordering;
pub use crate::iter::{Bound, Position, OuterPair};
use Bound::*;
use Position::*;
use OuterPair::*;

pub trait Seek {
    type Key: Ord + Copy;
    type Value;
    type Token;           // allows us to decouple seeking from producing values

    fn seek(&mut self, bound: Bound<Self::Key>) -> Position<Self::Key, Self::Token>;

    fn value(&self, token: Self::Token) -> Self::Value;
    // allows destination passing style. TBD if this is useful.
    fn put_value(&self, token: Self::Token, dest: &mut Self::Value) {
        *dest = self.value(token);
    }
}

pub struct Iter<S: Seek> {
    pub bound: Bound<S::Key>,
    pub seeker: S,
}

impl<S: Seek> Iterator for Iter<S> {
    type Item = (S::Key, S::Value);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.seeker.seek(self.bound) {
                Have(k, tok) => {
                    self.bound = Greater(k);
                    return Some((k, self.seeker.value(tok)));
                }
                Know(Done) => {
                    self.bound = Done;
                    return None;
                }
                Know(p) => { self.bound = p; }
            }
        }
    }
}


// ---------- INNER JOIN ----------
#[derive(Debug, Copy, Clone)]
pub struct Join<X,Y>(X, Y);

impl<X: Seek, Y:Seek<Key = X::Key>> Seek for Join<X, Y> {
    type Key = X::Key;
    type Value = (X::Value, Y::Value);
    type Token = (X::Token, Y::Token);

    fn seek(&mut self, bound: Bound<X::Key>) -> Position<X::Key, (X::Token, Y::Token)> {
        let p = self.0.seek(bound);
        let q = self.1.seek(p.bound());
        p.inner_join(q)
    }

    fn value(&self, token: (X::Token, Y::Token)) -> (X::Value, Y::Value) {
        (self.0.value(token.0), self.1.value(token.1))
    }
}


// ---------- OUTER JOIN ----------
#[derive(Debug, Copy, Clone)]
pub struct OuterJoin<X,Y>(X, Y);

impl<X: Seek, Y:Seek<Key = X::Key>> Seek for OuterJoin<X, Y> {
    type Key = X::Key;
    type Value = OuterPair<X::Value, Y::Value>;
    type Token = OuterPair<X::Token, Y::Token>;

    fn seek(&mut self, bound: Bound<X::Key>) -> Position<X::Key, OuterPair<X::Token, Y::Token>> {
        self.0.seek(bound).outer_join(self.1.seek(bound))
    }

    fn value(&self, token: OuterPair<X::Token, Y::Token>) -> OuterPair<X::Value, Y::Value> {
        match token {
            Left(x) => Left(self.0.value(x)),
            Right(y) => Right(self.1.value(y)),
            Both(x,y) => Both(self.0.value(x), self.1.value(y)),
        }
    }
}
