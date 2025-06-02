use std::mem;

// Non-empty forward-seekable iterators.
pub trait Seek: Sized {
    type Key: Ord;
    type Value;
    fn key(&self) -> Self::Key;
    fn value(&self) -> Self::Value;
    fn next(self) -> Option<Self>;
    fn seek(self, key: &Self::Key) -> Option<Self>;
}

#[allow(unused)]
struct SeekIter<T>(Option<T>);
impl<T: Seek> Iterator for SeekIter<T> {
    type Item = (T::Key, T::Value);
    fn next(&mut self) -> Option<Self::Item> {
        let seek = self.0.take()?;
        let result = (seek.key(), seek.value());
        self.0 = seek.next();
        return Some(result);
    }
}


// ---------- Seekable iterator over a sorted slice ----------
struct SliceSeek<'a, K, V> {
    elems: &'a [(K,V)],
    posn: usize,
}

impl<'a, K: Ord, V> Seek for SliceSeek<'a, K, V> {
    type Key = &'a K;
    type Value = &'a V;

    fn key(&self) -> &'a K { &self.elems[self.posn].0 }
    fn value(&self) -> &'a V { &self.elems[self.posn].1 }

    fn next(mut self) -> Option<Self> {
        self.posn += 1;
        if self.posn < self.elems.len() { Some(self) } else { None }
    }

    fn seek(mut self, key: &&'a K) -> Option<Self> {
        self.posn += self.elems[self.posn..].partition_point(|x| x.0 < **key);
        if self.posn < self.elems.len() { Some(self) } else { None }
    }
}


// ---------- LEAPFROG INTERSECTION ----------
pub struct Leapfrog<'a, Iter> {
    // invariant: all iterators are at the same key.
    iter: Iter,
    iters: &'a mut [Iter],
    posn: usize,
}

impl<'a, Iter: Seek> Leapfrog<'a, Iter> {
    // After advancing self.iter.key(), call search() to brings all iterators to
    // the same key, restoring our invariant.
    fn search(mut self) -> Option<Self> {
        let mut key = self.iter.key();
        let count = self.iters.len();
        let mut n = count;      // we count down to 0
        while n != 0 {
            // Swap the next iterator into "focus" (into self.iter).
            mem::swap(&mut self.iter, &mut self.iters[self.posn]);
            self.posn = (self.posn + 1) % self.iters.len();
            // Seek forward and check whether we have another match or need to
            // reset our count.
            self.iter = self.iter.seek(&key)?;
            let old_key = key;
            key = self.iter.key();
            n = if key == old_key { n - 1 } else { count };
        }
        Some(self)
    }
}

impl<'a, Iter: Seek> Seek for Leapfrog<'a, Iter> {
    type Key = Iter::Key;
    type Value = Box<[Iter::Value]>;

    // If we had a monoid instance on Iter::Value, we could maybe do this:
    // type Value = Iter::Value;

    fn key(&self) -> Iter::Key { self.iter.key() }

    fn value(&self) -> Box<[Iter::Value]> {
        self.iters[0..self.posn]
            .iter()
            .map(|it| it.value())
            .chain(std::iter::once(self.iter.value()))
            .chain(self.iters[self.posn..].iter().map(|it| it.value()))
            .collect()
    }

    fn next(mut self) -> Option<Self> {
        self.iter = self.iter.next()?;
        self.search()
    }

    fn seek(mut self, key: &Iter::Key) -> Option<Self> {
        self.iter = self.iter.seek(key)?;
        self.search()
    }
}
