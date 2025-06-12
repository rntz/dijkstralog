// TODO: try using Datafrog's galloping search implementation. I _think_ it's just a
// standard exponential-probe-to-find-range-then-binary-search-in-range, but it's very
// elegantly done:
//
// https://github.com/rust-lang/datafrog/blob/07bf407c740db506a56bcb4af3eb474eb83ca815/src/join.rs#L137
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
        // return self.partition_point(test);
        return recursive_gallop(self, test);
    }
}

pub fn gallop<X, F: FnMut(&X) -> bool>(elems: &[X], mut test: F) -> usize {
    let n = elems.len();
    let mut lo = 0;
    let mut hi = 1;
    loop {
        if hi >= n { hi = n; break; }
        if !test(unsafe { elems.get_unchecked(hi) }) { break; }
        lo = hi;
        hi *= 2;
    }
    // println!("searching {lo} .. {hi} in {elems:?}");
    return lo + elems[lo .. hi].partition_point(test);
}

// Performs better in my testing than gallop() but has worse worst-case behavior. I think
// O((log n)^2) instead of O(log n) but I haven't double-checked/proven it.
pub fn recursive_gallop<X, F: FnMut(&X) -> bool>(elems: &[X], mut test: F) -> usize {
    let n = elems.len();
    let mut lo = 0;

    if n == 0 { return 0 }
    if !test(&elems[0]) { return 0 }

    loop {
        debug_assert!(lo < n && test(&elems[lo]));
        let mut jump = 1;
        let mut hi = lo + jump;
        if hi >= n || !test(unsafe { elems.get_unchecked(hi) }) { return hi }
        // Exponential probing.
        loop {
            lo = hi;
            jump <<= 1;
            hi = lo + jump;
            if hi >= n || !test(unsafe { elems.get_unchecked(hi) }) { break }
        }
    }
}
