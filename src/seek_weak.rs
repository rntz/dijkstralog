// pub enum Bound {
//     AtLeast,
//     Greater,
// }

// maybe Key should be a type parameter here?
// then the same Seekable() could implement seeking for different key types?!?!
pub trait Seekable: Sized {
    type Key: Ord;
    type Value;

    fn key(&self) -> Self::Key;

    // A value, once produced, will not be visited again.
    fn seek(self, key: &Self::Key) -> Option<(Self, Self::Key, Option<Self::Value>)>;

    fn next(mut self) -> Option<(Self, Self::Key, Self::Value)> {
        let mut key = self.key();
        let mut vopt;
        loop {
            (self, key, vopt) = self.seek(&key)?;
            if let Some(val) = vopt {
                return Some((self, key, val));
            }
        }
    }
}

struct Intersect<I1, I2> {
    left: I1,
    right: I2,
    left_ge_right: bool,
}

impl<I1: Seekable, I2: Seekable<Key = I1::Key>> Seekable for Intersect<I1, I2> {
    type Key = I1::Key;
    type Value = (I1::Value, I2::Value);

    fn key(&self) -> I1::Key {
        debug_assert!(self.left_ge_right == (self.left.key() >= self.right.key()));
        if self.left_ge_right { self.left.key() } else { self.right.key() }
    }

    // #[allow(unused_assignments)]
    // fn seek(
    //     mut self,
    //     mut target: &I1::Key,
    // ) -> Option<(Self, Self::Key, Option<Self::Value>)> {
    //     target = std::cmp::max(target, &self.key());

    //     // Seek in the smaller first.
    //     // THIS IS WRONG.
    //     //
    //     // The problem is that I only have one bound: the maximum key of any
    //     // leaf iterator. But I want to start with the leaf iterator with the
    //     // minimum key, so I need the *minimum* key of all leaf iterators. But
    //     // this has no "semantic" meaning that I can see, so feels weird to
    //     // expose. The maximum key is at least a lower bound: "I have no
    //     // unvisited values below this key".
    //     //
    //     // Actually, who cares about visiting the smallest first. That does not
    //     // matter. What really matters is fairness; and that can be done
    //     // round-robin.
    //     //
    //     // BUT NO: if I just round-robin, then the arrangement of the tree
    //     // matters a lot; things nearer the root will get advanced more often!
    //     // blech!
    //     let mut key;
    //     let mut value_opt;

    //     if self.left_ge_right {
    //         (self.right, key, value_opt) = self.right.seek(target)?;
    //         target = &key;
    //         unimplemented!()
    //     } else {
    //         unimplemented!()
    //     }
    // }
    fn seek(self, target: &I1::Key) -> Option<(Self, Self::Key, Option<Self::Value>)> {
        unimplemented!()
    }
}
