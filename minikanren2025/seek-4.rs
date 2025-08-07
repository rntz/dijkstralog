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


// ---------- POSITIONS ----------
#[derive(PartialEq, Eq, Debug)]
enum Position<K, V> {
    Yield(K, Option<V>),
    Empty,
}
use Position::*;

impl<K: Ord, V> Position<K, V> {
    fn inner_join<U>(self: Position<K,V>, other: Position<K,U>) -> Position<K, (V,U)> {
        match (self, other) {
            (Empty, _) | (_, Empty) => Empty,
            (Yield(k1, x), Yield(k2, y)) if k1 == k2 => {
                Yield(k1, x.and_then(|xv| y.and_then(|yv| Some((xv,yv)))))
            }
            (Yield(k1, _), Yield(k2, _)) => Yield(k1.max(k2), None),
        }
    }
}


// ---------- SEEK ----------
trait Seek {
    type Key: Ord + Copy;
    type Value;
    fn posn(&self) -> Position<Self::Key, Self::Value>;
    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, test: F);
}

struct Keys<S: Seek>(S);
impl<S: Seek> Iterator for Keys<S> {
    type Item = S::Key;
    fn next(&mut self) -> Option<Self::Item> {
        while let Yield(k, maybe_v) = self.0.posn() {
            if let Some(_) = maybe_v {
                self.0.seek(|x| x > k);
                return Some(k)
            }
            self.0.seek(|x| x >= k)
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

    fn posn(&self) -> Position<X::Key, (X::Value, Y::Value)> {
        self.0.posn().inner_join(self.1.posn())
    }

    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, test: F) {
        self.0.seek(test);
        if let Yield(x, _) = self.0.posn() { // leapfrog!
            self.1.seek(|y| y >= x)
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
        if self.index >= self.elems.len() { Empty }
        else { Yield(self.elems[self.index], Some(())) }
    }

    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, mut test: F) {
        self.index += gallop(
            &self.elems[self.index..],
            |x| !test(*x)       // NOTE THE NEGATION!
        )
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
