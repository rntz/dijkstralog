use crate::iter::{*, Bound::*, Position::*};

// ---------- NEGATION / ANTIJOIN ----------
// TODO: TEST THIS!!!!
#[derive(Clone)]
pub struct Not<S: Seek> {
    iter: S,
    target: Bound<S::Key>,
}

impl<S: Seek> Not<S> {
    pub fn new(iter: S) -> Not<S> { Not { iter, target: Init } }
}

impl<S: Seek> Seek for Not<S> {
    type Key = S::Key;
    type Value = ();

    // This is ugly. How can we prettify this?
    #[inline(always)]
    fn posn(&self) -> Position<S::Key, ()> {
        match self.target {
            Init => Know(Init),
            Done => Know(Done),
            Greater(target) => Know(Greater(target)),
            Atleast(target) => match self.iter.posn() {
                Have(found, _) if target == found => Know(Greater(target)),
                Have(_found, _)                   => Have(target, ()),
                Know(Atleast(bound)) if target == bound => Know(Atleast(target)),
                Know(Atleast(_bound))                   => Have(target, ()),
                Know(Greater(_)) => Have(target, ()),
                Know(Done)       => Have(target, ()),
                Know(Init)       => unreachable!(),
            }
        }
    }

    #[inline(always)]
    fn seek(&mut self, target: Bound<S::Key>) {
        self.target = target;
        self.iter.seek(target);
    }
}

// Extension trait for Seek. Remove and integrate into Seek once no longer experimental.
#[allow(dead_code)]
trait SeekNot: Seek {
    // put this into "trait Seek" in iter.rs once this isn't experimental
    fn negate(self) -> Not<Self> where Self: Sized { return Not::new(self) }
}

impl<T: Seek> SeekNot for T {}
