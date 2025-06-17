// Functions for searching in sorted lists.

pub trait Search {
    type Item;
    fn search<F: FnMut(&Self::Item) -> bool>(&self, test: F) -> usize;
}

impl<X> Search for [X] {
    type Item = X;
    #[inline(always)]
    fn search<F: FnMut(&X) -> bool>(&self, test: F) -> usize {
        (&self).search(test)
    }
}

impl<'a, X> Search for &'a [X] {
    type Item = X;
    #[inline(always)]
    fn search<F: FnMut(&X) -> bool>(&self, test: F) -> usize {
        // ----------> DEFAULT SEARCH FUNCTION CHOICE GOES HERE <----------
        return careful_gallop(self, test);
        //return gallop(self, test);
        //return self.partition_point(test);
    }
}

// Exponential probing followed by binary search.
pub fn gallop_basic<X, F: FnMut(&X) -> bool>(elems: &[X], mut test: F) -> usize {
    let n = elems.len();
    let mut lo = 0;
    let mut hi = 1;
    loop {
        if hi >= n { hi = n; break; }
        if !test(unsafe { elems.get_unchecked(hi) }) { break; }
        lo = hi;
        hi *= 2;
    }
    return lo + elems[lo .. hi].partition_point(test);
}

// Same idea but inlining the binary search phase; slightly faster in my testing. Based on
// DataFrog's gallop(),
// https://github.com/rust-lang/datafrog/blob/07bf407c740db506a56bcb4af3eb474eb83ca815/src/join.rs#L137
pub fn gallop<X, F: FnMut(&X) -> bool>(elems: &[X], mut test: F) -> usize {
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

// Instead of falling back to binary search, recursively gallop-search the discovered
// region. Performs better in my testing than gallop() but has worse worst-case
// asymptotics. I think O((log k)^2) instead of O(log k) where k is the index of the
// sought-after key.
pub fn recursive_gallop<X, F: FnMut(&X) -> bool>(elems: &[X], mut test: F) -> usize {
    let n = elems.len();
    if n == 0 { return 0 }
    if !test(unsafe { elems.get_unchecked(0) }) { return 0 }

    let mut lo = 0;
    loop {
        debug_assert!(lo < n && test(&elems[lo]));
        let mut step = 1;
        let mut hi = lo + step;
        if hi >= n || !test(unsafe { elems.get_unchecked(hi) }) { return hi }
        // Exponential probing.
        loop {
            lo = hi;
            step <<= 1;
            hi = lo + step;
            if hi >= n || !test(unsafe { elems.get_unchecked(hi) }) { break }
        }
    }
}

// Attempts to ensure O(log k) worst case of gallop() while preserving the good practical
// performance of recursive_gallop().
pub fn careful_gallop<X, F: FnMut(&X) -> bool>(elems: &[X], mut test: F) -> usize {
    let n = elems.len();
    if n == 0 { return 0 }
    if !test(unsafe { elems.get_unchecked(0) }) { return 0 }

    let mut lo = 0;
    let mut loopcount = 0;
    loop {
        debug_assert!(lo < n && test(&elems[lo]));
        let mut step = 1;
        let mut hi = lo + step;
        if hi >= n || !test(unsafe { elems.get_unchecked(hi) }) { return hi }
        // Exponential probing.
        loop {
            lo = hi;
            step <<= 1;
            hi = lo + step;
            if hi >= n || !test(unsafe { elems.get_unchecked(hi) }) { break }
        }
        // Bail out to binary search after enough iterations.
        if loopcount > 16 {     // why is this the right magic number?!?!
            return lo+1 + elems[lo+1 .. hi.min(n)].partition_point(test);
        }
        loopcount += 1;
    }
}

// // Searching simultaneously for two predicates, one of which is more selective; this could
// // be used in iter::Ranges::seek(). In my testing it produced a small speedup over
// // gallop_basic(). I suspect I could further improve this by chasing the "separating
// // plane" intuition all the way through binary search. But to get a perf boost I'd also
// // need to adapt this to incorporate recursive_gallop() or careful_gallop()'s approach;
// // too much of a hassle right now.
//
// pub fn gallop2<X, F1, F2>(elems: &[X], mut test1: F1, mut test2: F2) -> (usize, usize)
// where F1: FnMut(&X) -> bool, F2: FnMut(&X) -> bool {
//     let n = elems.len();
//     let mut lo1 = 0;
//     let mut hi1 = 1;
//     // Find where test1 starts being false.
//     loop {
//         if hi1 >= n { hi1 = n; break; }
//         if !test1(unsafe { elems.get_unchecked(hi1) }) { break }
//         lo1 = hi1;
//         hi1 *= 2;
//     }
//     // Find where test2 starts being false.
//     let mut lo2 = lo1;
//     debug_assert!(lo2 >= elems.len() || test2(&elems[lo2]));
//     let mut hi2 = hi1;
//     loop {
//         if hi2 >= n { hi2 = n; break; }
//         if !test2(unsafe { elems.get_unchecked(hi2) }) { break }
//         lo2 = hi2;
//         hi2 *= 2;
//     }
//     // TODO: If they overlap, should we do something special??
//     return (lo1 + elems[lo1..hi1].partition_point(test1),
//             lo2 + elems[lo2..hi2].partition_point(test2))
// }
