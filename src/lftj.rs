// ---------- SEEKABLE ITERATORS ----------
pub trait Seek: Iterator {
    fn empty(&self) -> bool;
    // precondition for key() and advance(): !self.empty()
    fn key(&self) -> Self::Item;
    fn advance(&mut self);
    // seek() does not require !self.empty().
    fn seek(&mut self, key: Self::Item);

    fn here(&self) -> Option<Self::Item> {
        if self.empty() { None } else { Some(self.key()) }
    }
}

// an implementation of Iterator::next() in terms of Seek::{done,key,advance}.
fn seek_next<T: Seek>(this: &mut T) -> Option<T::Item> {
    if this.empty() { return None }
    let elem = this.key();
    this.advance();
    return Some(elem);
}

// ensure (&mut dyn Seek) implements Seek
impl<S: Seek + ?Sized> Seek for &mut S {
    #[inline] fn empty(&self) -> bool { (**self).empty() }
    #[inline] fn key(&self) -> S::Item { (**self).key() }
    #[inline] fn advance(&mut self) { (**self).advance() }
    #[inline] fn seek(&mut self, key: S::Item) { (**self).seek(key) }
    #[inline] fn here(&self) -> Option<S::Item> { (**self).here() }
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
    fn empty(&self) -> bool { self.posn >= self.elems.len() }

    fn key(&self) -> &'a T {
        debug_assert!(!self.empty());
        &self.elems[self.posn]
    }

    fn advance(&mut self) {
        debug_assert!(!self.empty());
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
pub struct Leapfrog<Iter>(Option<(usize, Vec<Iter>)>);

fn leapfrog_search<I: Seek>(mut index: usize, mut iters: Vec<I>) -> Option<(usize, Vec<I>)>
where I::Item: Eq
{
    let n = iters.len();
    let mut hi = iters[if index>0 {index-1} else {n-1}].key();
    loop {
        let iter = &mut iters[index];
        if hi == iter.key() { // all iters at same key
            return Some((index, iters));
        }
        // Hm, if seek() returned an Option<K> this would work fine? but
        // we'd have to get the same value again when we called next(); we
        // don't want to advance _past_ the element.
        iter.seek(hi);
        if iter.empty() { return None; }
        hi = iter.key();
        index = (index + 1) % n;
    }
}

impl<I: Seek> Leapfrog<I> where I::Item: Ord {
    // shouldn't I be able to implement this for any IntoIter thing?
    pub fn new(mut iters: Vec<I>) -> Self {
        // Intersecting nothing would produce "universal" relation, which I
        // can't represent efficiently.
        assert!(0 < iters.len());
        // If any iterator is already done, so are we.
        if iters.iter().any(|x| x.empty()) { return Leapfrog(None) }
        iters.sort_by_key(|it| it.key());
        return Leapfrog(leapfrog_search(0, iters))
    }
}

impl<S: Seek> Iterator for Leapfrog<S> where S::Item: Ord {
    type Item = S::Item;
    fn next(&mut self) -> Option<S::Item> { seek_next(self) }
}

impl<S: Seek> Seek for Leapfrog<S> where S::Item: Ord {
    fn empty(&self) -> bool { self.0.is_none() }

    fn key(&self) -> S::Item {
        debug_assert!(!self.empty());
        let (index, iters) = self.0.as_ref().unwrap();
        iters[*index].key()
    }

    fn here(&self) -> Option<S::Item> {
        let (index, iters) = self.0.as_ref()?;
        Some(iters[*index].key())
    }

    fn advance(&mut self) {
        debug_assert!(!self.empty());
        let (index, mut iters) = self.0.take().unwrap();
        let iter = &mut iters[index];
        iter.advance();
        self.0 = if iter.empty() { None } else {
            leapfrog_search((index+1) % iters.len(), iters)
        }
    }

    fn seek(&mut self, key: S::Item) {
        if let Some ((index, mut iters)) = self.0.take() {
            iters[index].seek(key);
            self.0 = leapfrog_search((index + 1) % iters.len(), iters);
        }
    }
}
