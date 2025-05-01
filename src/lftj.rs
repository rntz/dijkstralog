// ---------- SEEKABLE ITERATORS ----------
pub trait Seek: Iterator {
    fn done(&self) -> bool;
    // precondition for key() and advance(): !self.done()
    fn key(&self) -> Self::Item;
    fn advance(&mut self);
    fn seek(&mut self, key: Self::Item); // does not require !self.done()
}

// an implementation of Iterator::next() in terms of Seek::{done,key,advance}.
fn seek_next<T: Seek>(this: &mut T) -> Option<T::Item> {
    if this.done() { return None }
    let elem = this.key();
    this.advance();
    return Some(elem);
}

// ensure (&mut dyn Seek) implements Seek
impl<S: Seek + ?Sized> Seek for &mut S {
    #[inline] fn done(&self) -> bool { (**self).done() }
    #[inline] fn key(&self) -> S::Item { (**self).key() }
    #[inline] fn advance(&mut self) { (**self).advance() }
    #[inline] fn seek(&mut self, key: S::Item) { (**self).seek(key) }
}


// ---------- Seekable iterator over a sorted slice ----------
pub struct SliceSeek<'a, T> {
    // invariant: elems is sorted
    elems: &'a [T],
    posn: usize,
}

impl<'a, T: Ord> SliceSeek<'a, T> {
    pub fn new(list: &'a [T]) -> Self {
        debug_assert!(list.is_sorted());
        SliceSeek { elems: list, posn: 0 }
    }
}

impl<'a, T: Ord> Iterator for SliceSeek<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<&'a T> { seek_next(self) }
}

impl<'a, T: Ord> Seek for SliceSeek<'a, T> {
    fn done(&self) -> bool { self.posn >= self.elems.len() }

    fn key(&self) -> &'a T {
        debug_assert!(!self.done());
        &self.elems[self.posn]
    }

    fn advance(&mut self) {
        debug_assert!(!self.done());
        self.posn += 1;
    }

    fn seek(&mut self, key: &T) {
        debug_assert!(self.posn <= self.elems.len());
        self.posn += self.elems[self.posn..].partition_point(|x| x < key);
        debug_assert!(self.posn <= self.elems.len());
        debug_assert!(self.posn == self.elems.len() || key <= &self.elems[self.posn]);
    }
}


// ---------- LEAPFROG INTERSECTION ----------
pub struct Leapfrog<Iter> {
    iters: Vec<Iter>,
    idx: Option<u8>,            // better have less than 256 iterators!
    // idx: None means we are done.
}

impl<I: Seek>  Leapfrog<I> where I::Item: Ord {
    // shouldn't I be able to implement this for any IntoIter thing?
    pub fn new(mut iters: Vec<I>) -> Self {
        // Intersecting nothing would produce "universal" relation, which I
        // can't represent efficiently.
        assert!(0 < iters.len());
        assert!(iters.len() <= u8::MAX.into()); // too many iterators to fit in a u8

        // If any iterator is already done, so are we.
        if iters.iter().any(|x| x.done()) { return Leapfrog { iters, idx: None } }

        iters.sort_by_key(|it| it.key());
        let mut lf = Leapfrog { iters, idx: Some(0) };
        lf.search();
        lf
    }

    fn search(&mut self) {
        debug_assert!(!self.done());
        let mut idx = self.idx.unwrap() as usize;
        // rust has no modular arithmetic primitive, ugh.
        let n = self.iters.len();
        let mut hi: I::Item = self.iters[if idx>0 {idx} else {n} - 1].key();
        loop {
            let iter = &mut self.iters[idx];
            if hi == iter.key() { // all iters at same key
                self.idx = Some(idx as u8);
                return;
            }
            // Hm, if seek() returned an Option<K> this would work fine? but
            // we'd have to get the same value again when we called next(); we
            // don't want to advance _past_ the element.
            iter.seek(hi);
            if iter.done() { self.idx = None; return; }
            hi = iter.key();
            idx = (idx + 1) % n;
        }
    }
}

impl<S: Seek> Iterator for Leapfrog<S> where S::Item: Ord {
    type Item = S::Item;
    fn next(&mut self) -> Option<S::Item> { seek_next(self) }
}

impl<S: Seek> Seek for Leapfrog<S> where S::Item: Ord {
    fn done(&self) -> bool { self.idx.is_none() }

    fn key(&self) -> S::Item {
        debug_assert!(!self.done());
        self.iters[self.idx.unwrap() as usize].key()
    }

    fn advance(&mut self) {
        debug_assert!(!self.done());
        let idx = self.idx.unwrap() as usize;
        let iter = &mut self.iters[idx];
        iter.advance();
        if iter.done() { self.idx = None; return; }
        self.idx = Some(((idx + 1) % self.iters.len()) as u8);
        self.search();
    }

    fn seek(&mut self, key: S::Item) {
        let idx = self.idx.unwrap() as usize;
        let iter = &mut self.iters[idx];
        iter.seek(key);
        if iter.done() { self.idx = None; return; }
        self.idx = Some(((idx + 1) % self.iters.len()) as u8);
        self.search();
    }
}
