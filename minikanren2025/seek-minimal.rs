// ---------- SEEKABLE ITERATORS ----------
enum Position<S: Seek + ?Sized> {
    Yield(S::Key, Option<S::Value>),
    Empty,
}
use Position::*;

trait Seek {
    type Key: Ord + Copy;
    type Value;

    fn seek<F: FnMut(&Self::Key) -> bool>(&mut self, test: F) -> Position<Self>;

    fn join<T: Seek>(self, other: T) -> Join<Self, T> where Self: Sized {
        Join(self, other)
    }

    fn keys(mut self) -> Keys<Self> where Self: Sized {
        let posn = self.seek(|_| true);
        Keys { iter: self, posn }
    }
}

// ---------- INTERSECTION ("inner join") OF SEEKABLE ITERATORS ----------
struct Join<X,Y>(X, Y);

impl<X: Seek, Y: Seek<Key=X::Key>> Seek for Join<X,Y> {
    type Key   = X::Key;
    type Value = (X::Value, Y::Value);

    fn seek<F: FnMut(&Self::Key) -> bool>(&mut self, test: F) -> Position<Self> {
        let Yield(k0, r0) = self.0.seek(test) else { return Empty };
        let Yield(k1, r1) = self.1.seek(|k| *k >= k0) else { return Empty };
        if k0 == k1 { // We've found a key present in both iterators!
            Yield(k1, r0.and_then(|v0| r1.and_then(|v1| Some((v0,v1)))))
        } else {      // Not yet, keep looking.
            Yield(k1, None)
        }
    }
}

// ---------- ITERATING OVER THE KEYS OF A SEEKABLE ITERATOR ----------
struct Keys<S: Seek> { iter: S, posn: Position<S> }

impl<S: Seek> Iterator for Keys<S> {
    type Item = S::Key;
    fn next(&mut self) -> Option<Self::Item> {
        let mut posn = match self.posn {
            Empty => return None,
            Yield(k, Some(_)) => { self.posn = self.iter.seek(|x| *x > k); return Some(k); }
            Yield(k, None) => self.iter.seek(|x| *x >= k),
        };
        loop { posn = match posn {
            Empty => { self.posn = Empty; return None; }
            Yield(k, Some(_)) => { self.posn = self.iter.seek(|x| *x > k); return Some(k); }
            Yield(k, None) => self.iter.seek(|x| *x >= k),
        } }
    }
}

// ---------- SEEKABLE ITERATOR FOR SORTED LISTS ----------
struct Elements<'a, X> {
    elems: &'a [X],
    index: usize,
}

fn elements<X: Ord>(elems: &[X]) -> Elements<X> {
    Elements { elems, index: 0 }
}

impl<'a, X: Ord + Copy> Seek for Elements<'a, X> {
    type Key = X;
    type Value = ();

    fn seek<F: FnMut(&Self::Key) -> bool>(&mut self, test: F) -> Position<Self> {
        self.index += gallop(&self.elems[self.index..], test);
        if self.index >= self.elems.len() { Empty }
        else { Yield(self.elems[self.index], Some(())) }
    }
}

// Galloping search (exponential probing followed by binary search), based on DataFrog's gallop(),
// https://github.com/rust-lang/datafrog/blob/07bf407c740db506a56bcb4af3eb474eb83ca815/src/join.rs#L137
fn gallop<X, F: FnMut(&X) -> bool>(elems: &[X], mut test: F) -> usize {
    let n = elems.len();
    if n == 0 || test(&elems[0]) { return 0 }
    let mut lo = 0;
    let mut step = 1;
    while step < n - lo && !test(&elems[lo + step]) { // exponential probing phase
        lo += step;
        step <<= 1;
    }
    step >>= 1;
    while step > 0 {                                 // binary search phase
        if step < n - lo && !test(&elems[lo + step]) {
            lo += step;
        }
        step >>= 1;
    }
    return lo + 1;
}

// ---------- TOY BENCHMARK ----------
use std::time::{Instant, Duration};

const N: u32 = 1_000_000_000;   // make sure you compile with -O/--release!

fn timed<X, F: FnOnce() -> X>(f: F) -> (X, Duration) {
    let now = Instant::now();
    let result = f();
    return (result, now.elapsed());
}

fn ntimes<F: Fn()>(n: usize, f: F) {
    for _ in 0..n { f() }
}

// Hand-written intersection.
fn count_intersection(xs: &[u32], ys: &[u32]) -> usize {
    let xn = xs.len();
    let yn = ys.len();
    let mut count = 0;
    let mut i = 0;
    let mut j = 0;
    while i < xn {
        let x = xs[i];
        j += gallop(&ys[j..], |y| x <= *y);
        if j == yn { break }
        let y = ys[j];
        if x == y {
            count += 1;
            j += 1;
        }
        i += 1;
        i += gallop(&xs[i..], |x| y <= *x);
    }
    return count;
}

fn main() {
    use std::io::{stdout, Write};
    let now = Instant::now();
    print!("Constructing vectors... ");
    stdout().flush().expect("io error");

    let evens: Vec<u32>  = (0..=N).filter(|x| x % 2 == 0).collect();
    let odds:  Vec<u32>  = (0..=N).filter(|x| x % 2 == 1).collect();
    let threes: Vec<u32> = (0..=N).filter(|x| x % 3 == 0).collect();
    let ends:  Vec<u32>  = vec![0, N];

    let evens  = &evens[..];
    let odds   = &odds[..];
    let threes = &threes[..];
    let ends   = &ends[..];

    let elapsed = now.elapsed();
    println!("took {:.2}s", elapsed.as_secs_f32());
    println!(
        "{}M evens, {}M odds, {}M threes, {} ends",
        evens.len()  / 1_000_000,
        odds.len()   / 1_000_000,
        threes.len() / 1_000_000,
        ends.len(),
    );

    println!();
    ntimes(3, || {              // evens & ends - few elements, fast
        let (n, elapsed) = timed(|| count_intersection(evens, ends));
        println!("{:.2}s evens & ends ({n} elts) [handwritten]", elapsed.as_secs_f32());
        let (m, elapsed) = timed(|| elements(evens).join(elements(ends)).keys().count());
        println!("{:.2}s evens & ends ({m} elts)", elapsed.as_secs_f32());
        assert!(n == m);
    });

    println!();
    ntimes(3, || {              // evens & odds - no elements, slow
        let (n, elapsed) = timed(|| count_intersection(evens, odds));
        println!("{:.2}s evens & odds [handwritten]", elapsed.as_secs_f32());
        let (m, elapsed) = timed(|| elements(evens).join(elements(odds)).keys().count());
        println!("{:.2}s evens & odds", elapsed.as_secs_f32());
        assert!(n == 0);
        assert!(n == m);
    });

    println!();
    ntimes(3, || {              // evens & threes - many elements, slow
        let (n, elapsed) = timed(|| count_intersection(evens, threes));
        println!("{:.2}s evens & threes ({}M elts) [handwritten]",
                 elapsed.as_secs_f32(),
                 n / 1_000_000);
        let (m, elapsed) = timed(|| elements(evens).join(elements(threes)).keys().count());
        println!("{:.2}s evens & threes ({}M elts)", elapsed.as_secs_f32(), m / 1_000_000);
        assert!(n == m);
    });

    println!();
    ntimes(3, || {              // evens & odds & ends - no elements, fast
        let (n, elapsed) = timed(|| elements(evens).join(elements(odds)).join(elements(ends)).keys().count());
        println!("{:.2}s evens & odds & ends", elapsed.as_secs_f32());
        assert!(n == 0);
    });
}
