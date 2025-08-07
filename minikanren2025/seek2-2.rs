#![allow(dead_code, unused_imports)]
use std::cmp::Ordering;
use std::time::{Instant, Duration};

// Based on DataFrog's gallop(),
// https://github.com/rust-lang/datafrog/blob/07bf407c740db506a56bcb4af3eb474eb83ca815/src/join.rs#L137
fn gallop<X, F: FnMut(&X) -> bool>(elems: &[X], mut test: F) -> usize {
    let n = elems.len();
    if n == 0 || !test(&elems[0]) { return 0 }
    let mut lo = 0;
    let mut step = 1;
    while step < n - lo && test(&elems[lo + step]) {
        lo += step;
        step <<= 1;
    }
    step >>= 1;
    while step > 0 {
        if step < n - lo && test(&elems[lo + step]) {
            lo += step;
        }
        step >>= 1;
    }
    return lo + 1;
}


// ---------- BOUNDS ----------
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
enum Bound<K> {
    Atleast(K),
    Greater(K),
    Done,
}
use Bound::*;

impl<K: Ord> PartialOrd for Bound<K> {
    fn partial_cmp(&self, other: &Bound<K>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord> Ord for Bound<K> {
    fn cmp(&self, other: &Bound<K>) -> Ordering {
        match (self, other) {
            (Done, Done) => Ordering::Equal,
            (_   , Done) => Ordering::Less,
            (Done,    _) => Ordering::Greater,
            (Atleast(x)  ,  Atleast(y)) |
            (Greater(x)  ,  Greater(y)) => x.cmp(y),
            (Atleast(x)  ,  Greater(y)) => if x <= y { Ordering::Less } else { Ordering::Greater }
            (Greater(x)  ,  Atleast(y)) => if x <  y { Ordering::Less } else { Ordering::Greater }
        }
    }
}

impl<K: Ord> Bound<K> {
    fn matches(&self, other: K) -> bool { self <= &Atleast(other) }
}


// ---------- POSITIONS ----------
#[derive(PartialEq, Eq, Debug)]
enum Position<K, V> {
    Have(K, V),
    Know(Bound<K>),
}
use Position::*;

impl<K: Ord, V> Position<K, V> {
    // TODO: try to optimize this?
    fn inner_join<U>(self: Position<K,V>, other: Position<K,U>) -> Position<K, (V,U)> {
        match (self, other) {
            (Have(k, x), Have(k2, y)) if k == k2 => Have(k, (x, y)),
            (p, q) => Know(p.to_bound().max(q.to_bound())),
        }
    }
}

impl<K: Ord, V> Position<K,V> {
    fn to_bound(self) -> Bound<K> {
        match self {
            Have(k, _) => Atleast(k),
            Know(p)    => p,
        }
    }
}


// ---------- SEEK ----------
trait Seek {
    type Key: Ord + Copy;
    type Value;
    fn posn(&self) -> Position<Self::Key, Self::Value>;
    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, test: F) -> Position<Self::Key, Self::Value>;
}

struct Keys<S: Seek> {
    posn: Position<S::Key, S::Value>,
    iter: S,
}

impl<S: Seek> Keys<S> {
    fn new(iter: S) -> Keys<S> { Keys { posn: iter.posn(), iter } }
}

impl<S: Seek> Iterator for Keys<S> {
    type Item = S::Key;
    fn next(&mut self) -> Option<Self::Item> {
        let mut bound = match self.posn {
            Know(Done) => return None,
            Have(k, _) => { self.posn = self.iter.seek(|x| x > k); return Some(k); }
            Know(p) => p,
        };
        loop {
            match self.iter.seek(|x| bound.matches(x)) {
                done@Know(Done) => { self.posn = done; return None; }
                Have(k, _) => { self.posn = self.iter.seek(|x| x > k); return Some(k); }
                Know(p) => { bound = p; }
            }
        }
    }
}


// ---------- INNER JOIN ----------
#[derive(Clone)]
struct Join<X,Y>(X, Y);

impl<X: Seek, Y: Seek<Key=X::Key>> Seek for Join<X,Y> {
    type Key   = X::Key;
    type Value = (X::Value, Y::Value);

    fn posn(&self) -> Position<X::Key, (X::Value, Y::Value)> {
        self.0.posn().inner_join(self.1.posn())
    }

    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, test: F) -> Position<Self::Key, Self::Value> {
        match self.0.seek(test) {
            Know(p) => match self.1.seek(|x| p.matches(x)) {
                Know(q) => { assert!(q >= p); Know(q) }
                Have(k, _) => {assert!(Atleast(k) >= p); Know(Atleast(k)) }
            }
            Have(k, x) => match self.1.seek(|x| x >= k) {
                Know(q) => { assert!(q >= Atleast(k)); Know(q) }
                Have(k2, y) if k == k2 => Have(k, (x, y)),
                Have(k2, _) => { assert!(k2 >= k); Know(Atleast(k2)) }
            }
        }
    }
}


// ---------- SORTED LISTS ----------
#[derive(Clone)]
struct Elements<'a, X> {
    elems: &'a [X],
    index: usize,
}

fn elements<X: Ord + Copy>(elems: &[X]) -> Elements<X> { Elements { elems, index: 0 } }

impl<'a, X: Ord + Copy> Seek for Elements<'a, X> {
    type Key = X;
    type Value = ();

    fn posn(&self) -> Position<X, ()> {
        if self.index >= self.elems.len() { Know(Done) }
        else { Have(self.elems[self.index], ()) }
    }

    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, mut test: F) -> Position<X, ()> {
        self.index += gallop(
            &self.elems[self.index..],
            |x| !test(*x)       // NOTE THE NEGATION!
        );
        // TODO: if gallop() returned more info could we avoid this call and speed this
        // up?
        self.posn()
    }
}


// ---------- BENCHMARK ----------
const N: u32 = 1_000_000_000;

fn timed<X, F: FnOnce() -> X>(f: F) -> (X, Duration) {
    let now = Instant::now();
    let result = f();
    return (result, now.elapsed());
}

fn ntimes<F: Fn()>(n: usize, f: F) {
    for _ in 0..n { f() }
}

fn main() {
    use std::io::{stdout, Write};
    let now = Instant::now();
    print!("Constructing vectors... ");
    stdout().flush().expect("io error");

    let evens: Vec<u32> = (0..=N).filter(|x| x % 2 == 0).collect();
    let odds:  Vec<u32> = (0..=N).filter(|x| x % 2 == 1).collect();
    let ends:  Vec<u32> = vec![0, N];

    let evens = &evens[..];
    let odds  = &odds[..];
    let ends  = &ends[..];

    let elapsed = now.elapsed();
    println!("done! in {:.2}s", elapsed.as_secs_f32());

    println!("{:4}M evens", evens.len() / 1_000_000);
    println!("{:4}M odds",  odds.len()  / 1_000_000);
    println!(" {:4} ends",  ends.len());

    ntimes(3, || {
        let (n, elapsed) = timed(|| Keys::new(Join(elements(evens), elements(ends))).count());
        println!("{:.2}s evens & ends ({n} elts)", elapsed.as_secs_f32());
    });

    ntimes(3, || {
        let (_, elapsed) = timed(|| Keys::new(Join(elements(evens), elements(odds))).count());
        println!("{:.2}s evens & odds", elapsed.as_secs_f32());
    });
}
