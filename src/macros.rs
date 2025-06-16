#![allow(unused_macros)]

pub use dijkstralog_macros::*;

// Some declarative macros. Uglified because Rust doesn't have proper hygiene.

// A little nested loop language.
#[macro_export]
macro_rules! nest {
    { for $p:pat in $e:expr; $($rest:tt)* }
    => { for $p in $e { $crate::nest!($($rest)*); } };

    { if let $p:pat = $e:expr; $($rest:tt)* }
    => { if let $p = $e { $crate::nest!($($rest)*); } };

    { if $e:expr; $($rest:tt)* }
    => { if $e { $crate::nest!($($rest)*); } };

    // THE ABOVE MUST COME BEFORE THE BELOW, because statements overlap with our
    // syntax considerably and we want to prioritize our own syntax over
    // statement syntax.

    { $s:stmt; $($rest:tt)* } => { $s $crate::nest!($($rest)*) };
    { $s:stmt } => { $s };
    {} => {};
}

// TODO: support multiple relation semantics:
// - Set: use tuples() at lowest level with implicit value of ()
// - Bag: use ranges() at lowest level and .len() for value;
//        semantically, implicit value of 1 and combine with +
// - Map: explicit value type, tuples() at bottom.
// - maybe any monoid, use ranges() and combine with monoid function?
#[macro_export]
macro_rules! relationize {
    ($e:expr $(, $t:ty)*) => { $crate::__relationize_helper!($e, (), ($($t),*)) };
}

#[macro_export]
macro_rules! __relationize_helper {
    // We can interpret the sorted list as a "bag" by counting the number of
    // occurrences when we reach the bottom level.
    ($e:expr, ($($_:ty),*), ()) => { $e.len() };
    ($e:expr, ($($t1:ty),*), ($t:ty $(, $t2:ty)*)) => {
        $crate::iter::Seek::map(
            $crate::iter::ranges($e, $crate::__projection!(($($t1),*), ($($t2),*))),
            |xs| $crate::__relationize_helper!(xs, ($($t1,)* $t), ($($t2),*)))
    };
}

// projection!((), ())     => |(x,)|    x
// projection!((), (i8))   => |(x,_)|   x
// projection!((i8), ())   => |(_,x)|   x
// projection!((i8), (i8)) => |(_,x,_)| x
#[macro_export]
macro_rules! __projection {
    (($($t1:ty),*), ($($t2:ty),*)) =>
    { |&($($crate::__underscore!($t1),)* x, $($crate::__underscore!($t2),)*)| x }
}

#[macro_export]
macro_rules! __underscore { ($t:ty) => { _ } }
