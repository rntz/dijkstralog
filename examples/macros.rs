use dijkstralog::iter::{
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
        ranges($e, projection!(($($t1),*), ($($t2),*)))
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


// a little nested loop language
macro_rules! nest {
    { for $p:pat in $e:expr; $($rest:tt)* }
    => { for $p in $e { nest!($($rest)*); } };

    { if let $p:pat = $e:expr; $($rest:tt)* }
    => { if let $p = $e { nest!($($rest)*); } };

    { if $e:expr; $($rest:tt)* }
    => { if $e { nest!($($rest)*); } };

    // THE ABOVE MUST COME BEFORE THE BELOW, because statements overlap with our
    // syntax considerably and we want to prioritize our own syntax over
    // statement syntax.

    { $s:stmt; $($rest:tt)* } => { $s nest!($($rest)*) };
    { $s:stmt } => { $s };
    {} => {};
}

fn example_macro2() {
    let xs: &[&str] = &["johnny", "ursula"];
    let ys: &[&str] = &["bg", "kl"];
    assert!(xs.is_sorted());
    assert!(ys.is_sorted());

    let mut vs: Vec<(&str, &str)> = Vec::new();
    nest! {
        for k1 in xs; for k2 in ys;
        // for k1 in tuples(xs, |x| *x).keys();
        // for k2 in tuples(ys, |x| *x).keys();
        if k2 < k1;
        vs.push((k1, k2));
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
        vs.push((a, b, c, r * s * t));
    };
    println!("vs: {vs:?}");
}


// Language for producing nested Seeks. Smells graded-monadic. I should re-read
// "Relational Algebra by Way of Adjunctions".
//
// Handling statements here is complicated mainly because of seek_map, which
// needs to "skip over" all statements and find the next loop operator (yield,
// for, or if let) to figure out whether to call .map() or .filter_map().
macro_rules! seek {
    { yield $e:expr } => { $e };
    { for $p:pat in $e:expr; $($rest:tt)* } => { seek_map!($p, $e, $($rest)*) };
    // If we see an "if let", we have no choice but to generate an Option.
    // This is `fine': Options are still Seek-able if their contents are.
    { if let $p:pat = $e:expr; $($rest:tt)* }
    => { seek_filter!(if let $p = $e; $($rest)*) };
}

macro_rules! seek_map {
    {$p:pat, $e:expr, yield $e2:expr} => { $e.map(|$p| $e2) };
    {$p:pat, $e:expr, for $p2:pat in $e2:expr; $($rest:tt)*}
    => { $e.map(|$p| seek_map!($p2, $e2, $($rest)*)) };
    {$p:pat, $e:expr, if let $p2:pat = $e2:expr; $($rest:tt)*}
    => { $e.filter_map(|$p| seek_filter!(if let $p2 = $e2; $($rest)*)) };
}

macro_rules! seek_filter {
    { yield $e:expr } => { Some($e) };
    { for $p:pat in $e:expr; $($rest:tt)* } => { Some(seek_map!($p, $e, $($rest)*)) };
    { if let $p:pat = $e:expr; $($rest:tt)* }
    => { if let $p = $e { seek_filter!($($rest)*) } else { None } };
}

#[allow(non_snake_case)]
fn example_macro4() {
    let rAB: &[(&str, usize)] = &[("a", 1), ("a", 2), ("b", 1), ("b", 2)];
    let sBC: &[(usize, &str)] = &[(1, "one"), (1, "wun"), (2, "deux"), (2, "two")];
    let tAXC: &[(&str, i32, &str)] = &[("a", 17, "one"), ("b", 17, "deux"), ("mary", 23, "mary")];

    // generates nested Seek-erators.
    let r_ab = seek! {
        for bs in ranges(rAB, |t| t.0);
        for _t in tuples(bs,  |t| t.1);
        yield ()
    };

    // consumes nested Seek-erators (or anything else, really)
    let mut vs = Vec::new();
    nest! {
        for (a, r_b) in r_ab.iter();
        for (b, ())  in r_b.iter();
        vs.push((a, b));
    };
    println!("vs: {vs:?}");

    // Let's try a triangle join, expressed without relationizing first.
    // Problem: this isn't sharing work as much as it could - we re-compute
    // ranges(sBC) within an inner loop instead of doing it once and clone()ing.
    let triangle_iter = seek! {
        for (r_b, t_xc) in ranges(rAB, |t| t.0).join(ranges(tAXC, |t| t.0));
        if let Some(t_c) = ranges(t_xc, |t| t.1).lookup(17);
        for (_r, s_c) in tuples(r_b, |t| t.1).join(ranges(sBC, |t| t.0));
        for (_s, _t) in tuples(s_c, |t| t.1).join(tuples(t_c, |t| t.2));
        yield ()
    };
    let mut triangles = Vec::new();
    nest! {
        for (a, bcs) in triangle_iter.iter();
        for (b,  cs) in bcs.iter();
        for (c,  ()) in cs.iter();
        triangles.push((a,b,c));
    };
    println!("triangles: {triangles:?}");

    // All the way to a vector in one block.
    let mut triangles = Vec::new();
    nest! {
        for (a, (rb, txc)) in ranges(rAB, |t| t.0).join(ranges(tAXC, |t| t.0)).iter();
        if let Some(tc) = ranges(txc, |t| t.1).lookup(17);
        for (b, (_r, sc)) in tuples(rb, |t| t.1).join(ranges(sBC, |t| t.0)).iter();
        for (c, (_s, _t)) in tuples(sc, |t| t.1).join(tuples(tc, |t| t.2)).iter();
        triangles.push((a,b,c))
    }
    println!("triangles: {triangles:?}");
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
