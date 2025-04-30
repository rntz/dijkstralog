// a forward-seekable iterator
pub trait Seeker {
    type Key;
    fn done(&self) -> bool;
    // precondition for key(), next(), seek(): !self.done()
    fn key(&self) -> &Self::Key;
    fn next(&mut self);
    fn seek(&mut self, key: &Self::Key);
}

// impl<Key> Iterator for dyn Seeker<Key = Key> {
//     type Item = &Key;
//     fn next(&mut self) -> Option<Self::Item> {
//         unimplemented!();
//     }
// }

pub struct VectorSeek<'a, T> {
    // invariant: elems is sorted
    elems: &'a [T],
    posn: usize,
}

impl<'a, T: Ord> VectorSeek<'a, T> {
    pub fn new(list: &'a [T]) -> Self {
        debug_assert!(list.is_sorted());
        VectorSeek {
            elems: list,
            posn: 0,
        }
    }
}

impl<'a, T: Ord> Seeker for VectorSeek<'a, T> {
    type Key = T;

    fn done(&self) -> bool {
        self.posn > self.elems.len()
    }

    fn key(&self) -> &T {
        debug_assert!(!self.done());
        &self.elems[self.posn]
    }

    fn next(&mut self) {
        debug_assert!(!self.done());
        self.posn += 1;
    }

    fn seek(&mut self, key: &T) {
        debug_assert!(!self.done());
        self.posn += self.elems[self.posn..].partition_point(|x| x < key);
        debug_assert!(self.posn <= self.elems.len());
        debug_assert!(self.posn == self.elems.len() || key <= &self.elems[self.posn]);
    }
}
