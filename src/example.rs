use std::cmp::Ordering;

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

#[derive(Debug)]
pub struct SliceRangeSeek<'a, X, F> {
    elems: &'a [X],
    index_lo: usize,
    index_hi: usize,
    get_key: F,
}

impl<'a, X, K: Ord + Clone, F: Fn(&X) -> K> SliceRangeSeek<'a, X, F> {
    // TODO: test this!
    pub fn new(elems: &'a [X], get_key: F) -> SliceRangeSeek<'a, X, F> {
        let mut s = SliceRangeSeek { elems, index_lo: 0, index_hi: 0, get_key };
        s.seek_hi();
        s
    }

    fn seek_hi(&mut self) {
        self.index_hi = self.index_lo + if self.index_lo >= self.elems.len() { 0 } else {
            let key = (self.get_key)(&self.elems[self.index_lo]);
            self.elems[self.index_lo..].partition_point(|x| (self.get_key)(x) == key)
        };
    }

    pub fn posn(&self) -> Position<K, &'a [X]> {
        if self.index_lo >= self.elems.len() { return Know(Done) }
        let key = (self.get_key)(&self.elems[self.index_lo]);
        let xs = &self.elems[self.index_lo .. self.index_hi];
        return Have(key, xs);
    }

    pub fn seek(&mut self, target: &Bound<K>) {
        self.index_lo = self.index_hi + self.elems[self.index_hi..].partition_point(
            |x| !target.matches((self.get_key)(x))
        );
        self.seek_hi()
    }
}

pub fn main() {
    let xs: &[(isize, &str)] = &[(1, "one"), (1, "wun"), (2, "two"), (2, "deux")];
    let mut it = SliceRangeSeek::new(xs, |x| x.0);

    loop {
        let p = it.posn();
        let (lo, hi) = (it.index_lo, it.index_hi);
        println!("it: Range {{ {lo}, {hi} }} ");
        println!("p: {p:?}");

        match p {
            Have(k, _) => {
                println!("seeking Greater({k:?})");
                it.seek(&Greater(k));
            }
            Know(Done) => break,
            Know(p) => it.seek(&p),
        }
    }
}
