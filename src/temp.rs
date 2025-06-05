use crate::iter;
use crate::iter::{
    Seek,
    SliceRange,
    SliceBy,
};

pub fn main() {
    example_macro();
    // example2();
}

// ---------- GOAL 1: MACRO turning a LIST OF TUPLES into NESTED ITERATORS ----------
macro_rules! underscore { ($t:ty) => { _ } }
// projection!((), ())     => |(x,)|    x
// projection!((), (i8))   => |(x,_)|   x
// projection!((i8), ())   => |(_,x)|   x
// projection!((i8), (i8)) => |(_,x,_)| x
macro_rules! projection {
    (($($t1:ty),*), ($($t2:ty),*)) =>
    { |&($(underscore!($t1),)* x, $(underscore!($t2),)*)| x }
}

macro_rules! relationize_helper {
    // We can interpret the sorted list as a "bag" by counting the number of
    // occurrences when we reach the bottom level.
    ($e:expr, ($($_:ty),*), ()) => { $e.len() };
    ($e:expr, ($($t1:ty),*), ($t:ty $(, $t2:ty)*)) => {
        SliceRange::new($e, projection!(($($t1),*), ($($t2),*)))
            .map(|xs| relationize_helper!(xs, ($($t1,)* $t), ($($t2),*)))
    };
}

macro_rules! relationize {
    ($e:expr $(, $t:ty)*) => { relationize_helper!($e, (), ($($t),*)) };
}

fn example_macro() {
    let xs: &[(&str,)] = &[("a",)];
    let it = relationize!(xs, &str);
    let vs: Vec<_> = it.collect();
    println!("vs: {vs:?}");

    let xs: &[(&str, usize)] = &[("a", 0)];
    let it = relationize!(xs, &str, usize);
    let vs: Vec<(&str, Vec<_>)> = it.map(|x| x.collect()).collect();
    println!("vs: {vs:?}");
}

fn example2() {
    let r: &[(&str, usize, i8)] = &[("a", 1, 1), ("a", 2, 1), ("b", 1, 1), ("b", 2, 1)];
    // ↓ GOAL 1: GENERATE THIS CODE ↓
    let mut r_ab =
        SliceRange::new(r, |t| t.0)
        .map(|bs| SliceBy::new(bs, |t| t.1).map(|t| t.2));
    // ↑ GOAL 1: GENERATE THIS CODE ↑

    // Next goal is to generate this code:
    let mut vs = Vec::new();
    for (a, r_b) in r_ab.iter() {
        for (b, ann) in r_b.iter() {
            vs.push((a, b, ann));
        }
    }
    println!("vs: {vs:?}");
}


// // EXAMPLE 4: NATIVE ITERATION
// #[allow(non_snake_case)]
// fn example4() {
//     let rAB: &[(&str,  usize, i8)] = &[("a", 1, 1), ("a", 2, 2), ("b", 1, 1), ("b", 2, 2)];
//     let sBC: &[(usize, &str,  i8)] = &[(1, "one", 1), (1, "wun", 1), (2, "deux", 2), (2, "two", 2)];
//     let tAC: &[(&str,  &str,  i8)]  = &[("a", "one", 1), ("b", "deux", 2), ("mary", "mary", 3)];
//     assert!(rAB.is_sorted());
//     assert!(sBC.is_sorted());
//     assert!(tAC.is_sorted());

//     // Triangle query into a sorted vector.
//     let mut vs: Vec<(&str, usize, &str, i8)> = Vec::new();
//     let rt = SliceRange::new(rAB, |x| x.0).join(SliceRange::new(tAC, |x| x.0));
//     for (a, (rB, tC)) in rt.iter() {
//         let rs = SliceBy::new(rB, |x| x.1).join(SliceRange::new(sBC, |x| x.0));
//         for (b, (r, sC)) in rs.iter() {
//             let st = SliceBy::new(sC, |x| x.1).join(SliceBy::new(tC, |x| x.1));
//             for (c, (s, t)) in st.iter() {
//                 vs.push((a, b, c, r.2 * s.2 * t.2))
//             }
//         }
//     }

//     println!("vs: {vs:?}");
//     assert!(vs.is_sorted());
// }

// let's write a macro that turns something like this:

// (INSERT QUERY "PLAN" HERE)

// into the following:

// NESTED FOR LOOPS LIKE THE ABOVE
