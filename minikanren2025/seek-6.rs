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


// ---------- SEEK ----------
trait Seek {
    type Key: Ord + Copy;
    type Value;
    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, test: F) -> Position<Self>;
}

enum Position<S: Seek + ?Sized> {
    Yield(S::Key, Option<S::Value>),
    Empty,
}
use Position::*;


// ---------- KEYS ----------
struct Keys<S: Seek>(S);

impl<S: Seek> Iterator for Keys<S> {
    type Item = S::Key;
    fn next(&mut self) -> Option<Self::Item> {
        let mut p = self.0.seek(|_| true); // TODO: MAKE THIS FASTER
        while let Yield(k, maybe_v) = p {
            if let Some(_) = maybe_v {
                self.0.seek(|x| x > k);
                return Some(k)
            }
            p = self.0.seek(|x| x >= k);
        }
        return None
    }
}


// ---------- INNER JOIN ----------
#[derive(Clone)]
struct Join<X,Y>(X, Y);

impl<X: Seek, Y: Seek<Key=X::Key>> Seek for Join<X,Y> {
    type Key   = X::Key;
    type Value = (X::Value, Y::Value);

    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, test: F) -> Position<Self> {
        let Yield(kx, x) = self.0.seek(test) else { return Empty };
        let Yield(ky, y) = self.1.seek(|ky| ky >= kx) else { return Empty };
        debug_assert!(ky >= kx);
        if kx == ky {
            Yield(ky, x.and_then(|vx| y.and_then(|vy| Some((vx,vy)))))
        } else {
            Yield(ky, None)
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

    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, mut test: F) -> Position<Self> {
        self.index += gallop(
            &self.elems[self.index..],
            |x| !test(*x)       // NOTE THE NEGATION!
        );
        if self.index >= self.elems.len() { Empty }
        else { Yield(self.elems[self.index], Some(())) }
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
    let threes: Vec<u32> = (0..=N).filter(|x| x % 3 == 0).collect();
    let ends:  Vec<u32> = vec![0, N];

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
        let (n, elapsed) = timed(|| Keys(Join(elements(evens), elements(ends))).count());
        println!("{:.2}s evens & ends ({n} elts)", elapsed.as_secs_f32());
    });

    ntimes(3, || {
        let (_, elapsed) = timed(|| Keys(Join(elements(evens), elements(odds))).count());
        println!("{:.2}s evens & odds", elapsed.as_secs_f32());
    });

    ntimes(3, || {
        let (n, elapsed) = timed(|| Keys(Join(elements(evens), elements(threes))).count());
        println!("{:.2}s evens & threes ({}M elts)", elapsed.as_secs_f32(), n / 1_000_000);
    });
}
