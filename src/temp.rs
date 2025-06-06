use crate::iter;
use crate::iter::{
    Seek,
    ranges,
    tuples,
};

pub fn main() {
    println!("===== EXAMPLE MACRO =====");
    example_macro();

    println!("\n===== EXAMPLE MACRO 2 =====");
    example_macro2();

    println!("\n===== EXAMPLE MACRO 3 =====");
    example_macro3();

    println!("\n===== EXAMPLE MACRO 4 =====");
    example_macro4();

    println!("\n===== EXAMPLE 2 =====");
    example2();
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
        iter::ranges($e, projection!(($($t1),*), ($($t2),*)))
            .map(|xs| relationize_helper!(xs, ($($t1,)* $t), ($($t2),*)))
    };
}

macro_rules! relationize {
    ($e:expr $(, $t:ty)*) => { relationize_helper!($e, (), ($($t),*)) };
}

fn example_macro() {
    let xs: &[(&str,)] = &[("a",), ("a",)];
    let it = relationize!(xs, &str);
    let vs: Vec<_> = it.collect();
    println!("vs: {vs:?}");

    let xs: &[(&str, &str)] = &[("a", "A"), ("b", "B")];
    let it = relationize!(xs, &str, &str);
    let vs: Vec<(&str, Vec<_>)> = it.map(|x| x.collect()).collect();
    println!("vs: {vs:?}");
}

fn example2() {
    let r: &[(&str, usize)] = &[("a", 1), ("a", 2), ("b", 1), ("b", 2)];
    // ↓ GOAL 1: GENERATE THIS CODE ↓
    let mut r_ab =
        iter::ranges(r, |t| t.0)
        .map(|bs| tuples(bs, |t| t.1).map(|_| ()));
    // ↑ GOAL 1: GENERATE THIS CODE ↑

    // Next goal is to generate this code:
    let mut vs = Vec::new();
    for (a, r_b) in r_ab.iter() {
        for (b, ()) in r_b.iter() {
            vs.push((a, b));
        }
    }
    println!("vs: {vs:?}");
}


// a little nested loop language
macro_rules! nest {
    {} => {};

    { do $s:stmt; $($rest:tt)* }
    => { $s nest!($($rest)*) };

    { for $p:pat in $e:expr; $($rest:tt)* }
    => { for $p in $e { nest!($($rest)*); } };

    { if let $p:pat = $e:expr; $($rest:tt)* }
    => { if let $p = $e { nest!($($rest)*); } };

    { if $e:expr; $($rest:tt)* }
    => { if $e { nest!($($rest)*); } };

    { $b:block } => { $b };

    { $s:stmt } => { $s };
    { $s:stmt; } => { $s };
    { $s:stmt; $($rest:tt)* } => { $s nest!($($rest)*) }
}

fn example_macro2() {
    let xs: &[&str] = &["johnny", "ursula"];
    let ys: &[&str] = &["bg", "kl"];
    assert!(xs.is_sorted());
    assert!(ys.is_sorted());

    let mut vs: Vec<(&str, &str)> = Vec::new();
    nest! {
        for k1 in tuples(xs, |x| *x).keys();
        for k2 in tuples(ys, |x| *x).keys();
        if k2 < k1;
        vs.push((k1, k2))
    };
    println!("vs: {vs:?}");
}

// combining nest! with relationize!
#[allow(non_snake_case)]
fn example_macro3() {
    let rAB: &[(&str, usize)] = &[("a", 1), ("a", 2), ("b", 1), ("b", 2)];
    let sBC: &[(usize, &str)] = &[(1, "one"), (1, "wun"), (2, "deux"), (2, "two")];
    let tAXC: &[(&str, i32, &str)] = &[("a", 17, "one"), ("b", 17, "deux"), ("mary", 23, "mary")];

    let r_ab  = relationize!(rAB, &str, usize);
    let s_bc  = relationize!(sBC,       usize,      &str);
    let t_axc = relationize!(tAXC, &str,       i32, &str);

    // Unfortunately we still need clone()s in (somewhat) unpredictable places.
    // Is there a way to automate this?
    let mut vs = Vec::<(&str, usize, &str, usize)>::new();
    nest! {
        for (a, (r_b, t_xc)) in r_ab.join(t_axc).iter();
        if let Some(t_c)     =  t_xc.lookup(17);
        for (b, (r, s_c))    in r_b.join(s_bc.clone()).iter();
        for (c, (s, t))      in s_c.join(t_c.clone()).iter();
        do vs.push((a, b, c, r * s * t));
    };
    println!("vs: {vs:?}");

    // // mock-up of how seek! should look
    // let result = seek! {
    //     for (r_b, t_xc) in r_ab.join(t_axc);
    //     if let Some(t_c) = t_xc.lookup(17);
    //     for (r, s_c) in r_b.join(s_bc.clone());
    //     for (c, (s, t)) in s_c.join(t_c.clone());
    //     yield r * s * t
    // };
}


macro_rules! seek {
    { yield $e:expr } => { $e };
    { for $p:pat in $e:expr; $($rest:tt)* } => { seek_helper!(direct, $e, $p, $($rest)*) }
}

macro_rules! seek_helper {
    {direct, $seeker:expr, $p:pat, yield $e:expr} => { $seeker.map(|$p| $e) };
    {direct, $seeker:expr, $p:pat, for $p2:pat in $e:expr; $($rest:tt)*}
    => { $seeker.map(|$p| seek_helper!(direct, $e, $p2, $($rest)*)) };
    {direct, $seeker:expr, $p:pat, if let $p2:pat = $e:expr; $($rest:tt)*}
    => { $seeker.filter_map(|$p| if let $p2 = $e { seek_helper!(filter, $($rest)*) }
                                 else { None }) };

    {filter, yield $e:expr} => { Some($e) };
    {filter, for $p:pat in $e:expr; $($rest:tt)*}
    => { Some(seek_helper!(direct, $e, $p, $($rest)*)) };
    {filter, if let $p:pat = $e:expr; $($rest:tt)*}
    => { if let $p = $e { seek_helper!(filter, $($rest)*) } else { None } };
}

#[allow(non_snake_case)]
fn example_macro4() {
    let rAB: &[(&str, usize)] = &[("a", 1), ("a", 2), ("b", 1), ("b", 2)];
    let sBC: &[(usize, &str)] = &[(1, "one"), (1, "wun"), (2, "deux"), (2, "two")];
    let tAXC: &[(&str, i32, &str)] = &[("a", 17, "one"), ("b", 17, "deux"), ("mary", 23, "mary")];

    // generates nested Seek-erators
    let r_ab = seek! {
        for bs in ranges(rAB, |t| t.0);
        for cs in tuples(bs, |t| t.1);
        yield ()
    };

    // consumes nested Seek-erators (or anything else, really)
    let mut vs = Vec::new();
    nest! {
        for (a, r_b) in r_ab.iter();
        for (b, ())  in r_b.iter();
        do vs.push((a, b));
    };
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
//     let rt = ranges(rAB, |x| x.0).join(ranges(tAC, |x| x.0));
//     for (a, (rB, tC)) in rt.iter() {
//         let rs = tuples(rB, |x| x.1).join(ranges(sBC, |x| x.0));
//         for (b, (r, sC)) in rs.iter() {
//             let st = tuples(sC, |x| x.1).join(tuples(tC, |x| x.1));
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
