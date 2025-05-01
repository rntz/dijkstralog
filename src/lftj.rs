// ---------- SEEKABLE ITERATORS ----------
pub trait Seek: Iterator {
    fn done(&self) -> bool;
    // precondition for key() and advance(): !self.done()
    fn key(&self) -> Self::Item;
    fn advance(&mut self);
    // seek() does not require !self.done(). Note that if seek() returns
    // Some(item), then next() will return Some(item) again; we DO NOT advance
    // past the item we return.
    fn seek(&mut self, key: Self::Item) -> Option<Self::Item>;
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
    #[inline] fn seek(&mut self, key: S::Item) -> Option<S::Item> { (**self).seek(key) }
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

    fn seek(&mut self, key: &T) -> Option<&'a T> {
        debug_assert!(self.posn <= self.elems.len());
        self.posn += self.elems[self.posn..].partition_point(|x| x < key);
        debug_assert!(self.posn <= self.elems.len());
        if self.posn == self.elems.len() { None }
        else {
            let elem = &self.elems[self.posn];
            debug_assert!(key <= elem);
            Some(elem)
        }
    }
}


// ---------- LEAPFROG INTERSECTION ----------
pub enum Leapfrog<Iter> {
    Stop,
    Go { index: usize, iters: Vec<Iter> },
}

use Leapfrog::{Stop,Go};

impl<I: Seek> Leapfrog<I> where I::Item: Ord {
    // shouldn't I be able to implement this for any IntoIter thing?
    pub fn new(mut iters: Vec<I>) -> Self {
        // Intersecting nothing would produce "universal" relation, which I
        // can't represent efficiently.
        assert!(0 < iters.len());

        // If any iterator is already done, so are we.
        if iters.iter().any(|x| x.done()) { return Stop }

        iters.sort_by_key(|it| it.key());
        let mut lf = Go { index: 0, iters };
        lf.search();
        lf
    }

    fn unwrap(&self) -> (usize, &Vec<I>) {
        match self {
            Stop => panic!("Leapfrog::look() called but iterator is done"),
            Go { index, iters } => (*index, iters),
        }
    }

    fn take(&mut self) -> (usize, Vec<I>) {
        match std::mem::replace(self, Stop) {
            Stop => panic!("Leapfrog::take() called but iterator is done"),
            Go { index, iters } => (index, iters)
        }
    }

    fn search(&mut self) -> Option<I::Item> {
        let (mut index, mut iters) = self.take();
        // rust has no modular arithmetic primitive, ugh.
        let n = iters.len();
        let mut hi = iters[if index>0 {index-1} else {n-1}].key();
        loop {
            let iter = &mut iters[index];
            if hi == iter.key() { // all iters at same key
                *self = Go { index, iters };
                return Some(hi);
            }
            // Hm, if seek() returned an Option<K> this would work fine? but
            // we'd have to get the same value again when we called next(); we
            // don't want to advance _past_ the element.
            iter.seek(hi);
            if iter.done() { *self = Stop; return None; }
            hi = iter.key();
            index = (index + 1) % n;
        }
    }
}

impl<S: Seek> Iterator for Leapfrog<S> where S::Item: Ord {
    type Item = S::Item;
    fn next(&mut self) -> Option<S::Item> { seek_next(self) }
}

impl<S: Seek> Seek for Leapfrog<S> where S::Item: Ord {
    #[inline]
    fn done(&self) -> bool {
        match self { Stop => true, Go {..} => false }
    }

    fn key(&self) -> S::Item {
        debug_assert!(!self.done());
        let (index, iters) = self.unwrap();
        iters[index].key()
    }

    fn advance(&mut self) {
        debug_assert!(!self.done());
        let (mut index, mut iters) = self.take();
        let iter = &mut iters[index];
        iter.advance();
        if iter.done() {
            *self = Stop;
        } else {
            index = (index+1) % iters.len();
            *self = Go { iters, index };
            self.search();
        }
    }

    fn seek(&mut self, key: S::Item) -> Option<S::Item> {
        match self {
            Stop => None,
            Go { index, iters } => match iters[*index].seek(key) {
                None => { *self = Stop; None }
                Some(_) => {
                    *index = (*index + 1) % iters.len();
                    self.search()
                }
            }
        }
    }
}
