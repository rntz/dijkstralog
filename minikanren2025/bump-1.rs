#![allow(unused_imports, dead_code)]

fn main() { println!("foo"); }

use std::time::{Instant, Duration};

// Finds the first element of elems satisfying test().
// test() must be monotone: i <= j and test(elems[i]) imply test(elems[j]).
// Based on DataFrog's gallop(),
// https://github.com/rust-lang/datafrog/blob/07bf407c740db506a56bcb4af3eb474eb83ca815/src/join.rs#L137
fn gallop<X: Copy, F: FnMut(X) -> bool>(elems: &[X], mut test: F) -> usize {
    let n = elems.len();
    if n == 0 || test(elems[0]) { return 0 }
    let mut lo = 0;
    let mut step = 1;
    while step < n - lo && !test(elems[lo + step]) { // exponential probing phase
        lo += step;
        step <<= 1;
    }
    step >>= 1;
    while step > 0 {
        if step < n - lo && !test(elems[lo + step]) { // binary search phase
            lo += step;
        }
        step >>= 1;
    }
    return lo + 1;
}


// Leapfrog intersection.
fn count_intersection(xs: &[u32], ys: &[u32]) -> usize {
    let xn = xs.len();
    if xn == 0 { return 0 }
    let yn = ys.len();
    let mut count = 0;
    let mut i = 0;
    let mut j = 0;
    let mut x = xs[0];
    loop {
        // Leapfrog ys past xs.
        j += gallop(&ys[j..], |y| x <= y);
        if j >= yn { break }
        let y = ys[j];
        if x == y { count += 1; }
        i += 1;
        // Leapfrog xs past ys.
        i += gallop(&xs[i..], |x| y <= x);
        if i >= xn { break }
        x = xs[i];
        if x == y { count += 1; }
        j += 1;
    }
    return count;
}

// 3-way leapfrog
fn count_intersection3(xs: &[u32], ys: &[u32], zs: &[u32]) -> usize {
    let xn = xs.len();
    if xn == 0 { return 0 }
    let yn = ys.len();
    let zn = zs.len();

    let mut xi = 0;
    let mut x = xs[0];

    // Make sure zs >= ys >= xs.
    let mut yi = gallop(ys, |y| x <= y);
    if yi >= yn { return 0 }
    let mut y = ys[yi];

    let mut zi = 0;
    let mut count = 0;
    loop {
        // Leapfrog zs past ys so that x <= y <= z.
        zi += gallop(&zs[zi..], |z| y <= z);
        if zi >= zn { break }
        let z = zs[zi];
        if z == x { count += 1; } // if hi = lo
        xi += 1;                  // bump lo.

        // Leapfrog xs past zs so that y <= z <= x.
        xi += gallop(&xs[xi..], |x| z <= x);
        if xi >= xn { break }
        x = xs[xi];
        if x == y { count += 1; } // if hi = lo
        yi += 1;                  // bump lo

        // Leapfrog ys past xs so that z <= x <= y.
        yi += gallop(&ys[yi..], |y| x <= y);
        if yi >= yn { break }
        y = ys[yi];
        if y == z { count += 1; } // if hi = lo
        zi += 1;                  // bump lo
    }

    return count;
}


// ---------- SEEK ----------
struct Position<S: Seek + ?Sized> {
    key: S::Key,
    found: Option<S::Value>,
}

trait Seek {
    type Key: Ord + Copy;
    type Value;

    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, test: F) -> Option<Position<Self>>;

    fn count(mut self) -> usize where Self: Sized {
        let Some(mut state) = self.seek(|_| true) else { return 0 };
        let mut count = 0;
        loop {
            if state.found.is_some() { count += 1 }
            state = match self.seek(|x| x > state.key) {
                Some(state) => state,
                None => return count,
            };
        }
    }
}


// ---------- INNER JOIN ----------
#[derive(Clone)]
struct Join<X,Y> {
    x: X,
    y: Y,
    state: Either<Position<X>, Position<Y>>,
}

impl<X: Seek, Y: Seek<Key=X::Key>> Join<X,Y> {
    fn new(x: X, y: Y) -> Self {
        Join { x, y, state: Left(x.seek(|_| true)) }
    }
}

impl<X: Seek, Y: Seek<Key=X::Key>> Seek for Join<X,Y> {
    type Key   = X::Key;
    type Value = (X::Value, Y::Value);

    fn seek<F: FnMut(Self::Key) -> bool>(&mut self, test: F) -> Position<Self> {
        match self.state {
            Left(Empty) | Right(Empty) => Empty,
            Left(Yield(x, x_found)) => {
                let Yield(y, y_found) = self.y.seek(|y| y >= x)
                else { self.state = Right(Empty); return Empty; }

                match y_found {
                    None => 
                }

                self.state = 

                return Yield(y, found);

                match self.y.seek(|y| y >= x) {
                    Empty => { self.state = Right(Empty); return Empty; }
                    Yield(y, None) => {
                        self.state = Right(Yield(y, None));
                        return Yield(y, None);
                    }
                    Yield(y, Some(y_val)) => {
                        if x == y {
                            if let Some(x_val) = x_found {
                                return Yield(y, Some((x_val, y_val)))
                            }
                        }
                        match x_found {
                            None => {
                                self.state = Right(Yield(y, Some(y_val)));
                                return Yield(y, None);
                            }
                        }
                    }
                }
            }
        }
    }
}

// 
// // ---------- SORTED LISTS ----------
// #[derive(Clone)]
// struct Elements<'a, X> {
//     elems: &'a [X],
//     index: usize,
// }

// fn elements<X: Ord + Copy>(elems: &[X]) -> Elements<X> { Elements { elems, index: 0 } }

// impl<'a, X: Ord + Copy> Seek for Elements<'a, X> {
//     type Key = X;
//     type Value = ();

//     fn seek<F: FnMut(Self::Key) -> bool>(&mut self, mut test: F) -> Position<Self> {
//         self.index += gallop(&self.elems[self.index..], test);
//         if self.index >= self.elems.len() { Empty }
//         else { Yield(self.elems[self.index], Some(())) }
//     }
// }

// 
// // ---------- BENCHMARK ----------
// const N: u32 = 1_000_000_000;

// fn timed<X, F: FnOnce() -> X>(f: F) -> (X, Duration) {
//     let now = Instant::now();
//     let result = f();
//     return (result, now.elapsed());
// }

// fn ntimes<F: Fn()>(n: usize, f: F) {
//     for _ in 0..n { f() }
// }

// fn main() {
//     use std::io::{stdout, Write};
//     let now = Instant::now();
//     print!("Constructing vectors... ");
//     stdout().flush().expect("io error");

//     let evens: Vec<u32>  = (0..).map(|x| x * 2)    .take_while(|x| *x <= N).collect();
//     let odds:  Vec<u32>  = (0..).map(|x| 1 + x * 2).take_while(|x| *x <= N).collect();
//     let threes: Vec<u32> = (0..).map(|x| x * 3)    .take_while(|x| *x <= N).collect();
//     let ends:  Vec<u32>  = vec![0, N];

//     let evens = &evens[..];
//     let odds  = &odds[..];
//     let threes = &threes[..];
//     let ends  = &ends[..];

//     let elapsed = now.elapsed();
//     println!("took {:.2}s", elapsed.as_secs_f32());
//     println!(
//         "{}M evens, {}M odds, {}M threes, {} ends",
//         evens.len() / 1_000_000,
//         odds.len()  / 1_000_000,
//         threes.len()  / 1_000_000,
//         ends.len(),
//     );

//     ntimes(3, || {
//         let (n, elapsed) = timed(|| Keys::new(Join(elements(evens), elements(ends))).count());
//         println!("{:.2}s evens & ends ({n} elts)", elapsed.as_secs_f32());
//     });

//     ntimes(3, || {
//         let (_, elapsed) = timed(|| Keys::new(Join(elements(evens), elements(odds))).count());
//         println!("{:.2}s evens & odds", elapsed.as_secs_f32());
//     });

//     ntimes(3, || {
//         let (n, elapsed) = timed(|| Keys::new(Join(elements(evens), elements(threes))).count());
//         println!("{:.2}s evens & threes ({}M elts)", elapsed.as_secs_f32(), n / 1_000_000);
//     });
// }
