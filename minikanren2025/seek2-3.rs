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
#[allow(dead_code)] // allow non-use of Greater
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


// ---------- SEEK ----------
trait Seek {
    type Key: Ord + Copy;
    type Value;
    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, test: F) -> Position<Self::Key, Self::Value>;
}

struct Keys<S: Seek> {
    posn: Position<S::Key, S::Value>,
    iter: S,
}

impl<S: Seek> Keys<S> {
    fn new(mut iter: S) -> Keys<S> {
        let posn = iter.seek(|_| true);
        Keys { posn, iter }
    }
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
                Know(Done) => { self.posn = Know(Done); return None; }
                Have(k, _) => { self.posn = self.iter.seek(|x| x > k); return Some(k); }
                Know(p) => { bound = p; }
            }
        }
    }
}


// ---------- INNER JOIN ----------
struct Join<X, Y>(X, Y);

macro_rules! unsafe_assert {
    ($expr:expr) => {
        debug_assert!($expr);
        // // This seems to have no affect on performance.
        // if !($expr) { unsafe { core::hint::unreachable_unchecked() } }
    }
}

impl<X: Seek, Y: Seek<Key=X::Key>> Seek for Join<X,Y> {
    type Key   = X::Key;
    type Value = (X::Value, Y::Value);

    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, test: F) -> Position<Self::Key, Self::Value> {
        match self.0.seek(test) {
            Know(b0) => match self.1.seek(|x| b0.matches(x)) {
                Know(b1) => { unsafe_assert!(b1 >= b0); Know(b1) }
                Have(k1, _) => { unsafe_assert!(Atleast(k1) >= b0); Know(Atleast(k1)) }
            }
            Have(k0, v0) => match self.1.seek(|x| x >= k0) {
                Know(b1) => { unsafe_assert!(b1 >= Atleast(k0)); Know(b1) }
                Have(k1, v1) if k0 == k1 => Have(k1, (v0, v1)),
                Have(k1, _) => { unsafe_assert!(k1 >= k0); Know(Atleast(k1)) }
            }
        }
    }
}


// ---------- SORTED LISTS ----------
struct Elements<'a, X> {
    elems: &'a [X],
    index: usize,
}

fn elements<X: Ord + Copy>(elems: &[X]) -> Elements<X> { Elements { elems, index: 0 } }

impl<'a, X: Ord + Copy> Seek for Elements<'a, X> {
    type Key = X;
    type Value = ();

    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, mut test: F) -> Position<X, ()> {
        self.index += gallop(
            &self.elems[self.index..],
            |x| !test(*x)       // NOTE THE NEGATION!
        );
        if self.index >= self.elems.len() { Know(Done) }
        else { Have(self.elems[self.index], ()) }
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

    let evens: Vec<u32>  = (0..).map(|x| x * 2)    .take_while(|x| *x <= N).collect();
    let odds:  Vec<u32>  = (0..).map(|x| 1 + x * 2).take_while(|x| *x <= N).collect();
    let threes: Vec<u32> = (0..).map(|x| x * 3)    .take_while(|x| *x <= N).collect();
    let ends:  Vec<u32>  = vec![0, N];

    let evens = &evens[..];
    let odds  = &odds[..];
    let threes = &threes[..];
    let ends  = &ends[..];

    let elapsed = now.elapsed();
    println!("took {:.2}s", elapsed.as_secs_f32());
    println!(
        "{}M evens, {}M odds, {}M threes, {} ends",
        evens.len() / 1_000_000,
        odds.len()  / 1_000_000,
        threes.len()  / 1_000_000,
        ends.len(),
    );

    ntimes(3, || {
        let (n, elapsed) = timed(|| Keys::new(Join(elements(evens), elements(ends))).count());
        println!("{:.2}s evens & ends ({n} elts)", elapsed.as_secs_f32());
    });

    ntimes(3, || {
        let (_, elapsed) = timed(|| Keys::new(Join(elements(evens), elements(odds))).count());
        println!("{:.2}s evens & odds", elapsed.as_secs_f32());
    });

    ntimes(3, || {
        let (n, elapsed) = timed(|| Keys::new(Join(elements(evens), elements(threes))).count());
        println!("{:.2}s evens & threes ({}M elts)", elapsed.as_secs_f32(), n / 1_000_000);
    });
}
