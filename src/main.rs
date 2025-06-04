#![allow(dead_code, unused_imports, unused_variables, unused_mut)]

// old code that we don't need but still want to compile
mod lftj;
mod example;

// new code
mod iters;

use iters::{
    Position,
    Position::{*},
    Bound,
    Bound::{*},
    SliceRange,
    SliceBy,
    Seek,
};

// EXAMPLE 1: TRIES
fn example1() {
    let xs: &[(isize, &str)] = &[(1, "one"), (1, "wun"), (2, "two"), (2, "deux")];

    // Iterate through the slices on the first component of the tuples.
    let mut it = SliceRange::new(xs, |x| x.0);
    loop {
        let p = it.posn();
        println!("{p:?}");
        println!("{it:?}");
        println!();
        match p {
            Know(Done) => break,
            Know(p) => it.seek(p),
            Have(k, _) => it.seek(Greater(k)),
        }
    }

    // Dump it into a trie using nested iteration.
    let trie: Vec<(isize, Vec<&str>)> =
        SliceRange::new(xs, |tuple| tuple.0)
        .map(|_, tuples|
             SliceBy::new(tuples, |tuple| tuple.1)
             .collect_with(|y, _tuple| y))
        .collect();
    println!("trie: {trie:?}");
}

// EXAMPLE 2: TRIANGLE QUERY
fn example2() {
    let r: &[(&str, isize)] = &[("a", 1), ("a", 2), ("b", 1), ("b", 2)];
    let s: &[(isize, &str)] = &[(1, "one"), (1, "wun"), (2, "deux"), (2, "two")];
    let t: &[(&str, &str)]  = &[("a", "one"), ("b", "deux"), ("mary", "mary")];
    assert!(r.is_sorted());
    assert!(s.is_sorted());
    assert!(t.is_sorted());

    let mut r_ab =
        SliceRange::new(r, |t| t.0).map(|_, bs| SliceRange::new(bs, |t| t.1).map(|_, _| 2));
    let mut s_bc =
        SliceRange::new(s, |t| t.0).map(|_, cs| SliceRange::new(cs, |t| t.1).map(|_, _| 3));
    let mut t_ac =
        SliceRange::new(t, |t| t.0).map(|_, cs| SliceRange::new(cs, |t| t.1).map(|_, _| 5));

    let rtrie = r_ab.clone().collect_with(|a, bs| {
        (a, bs.collect_with(|b, v| (b, v)))
    });
    println!("rtrie: {rtrie:?}");

    // Let's plan a triangle query!
    // this requires we pay obeisance to the borrow checking gods.
    // I worry about what the closures look like...
    // I should try disassembly, but it might be too big to understand.
    let triangle_it =
        r_ab.join(t_ac).map_value(move |(r_b, t_c)| {
            r_b.join(s_bc.clone()).map_value(move |(r, s_c)| {
                s_c.join(t_c.clone()).map_value(move |(s, t)| r * s * t)
            })
        });

    let triangles = triangle_it
        .map(|_, bcs| bcs.map(|_, cs| cs.collect()).collect())
        .collect();
    println!("triangles: {triangles:?}");
}

// EXAMPLE 3: SEMIRING TRIANGLE QUERY WITH LESS move/clone
#[allow(non_snake_case)]
fn example3() {
    let rAB: &[(&str,  usize, i8)] = &[("a", 1, 1), ("a", 2, 2), ("b", 1, 1), ("b", 2, 2)];
    let sBC: &[(usize, &str,  i8)] = &[(1, "one", 1), (1, "wun", 1), (2, "deux", 2), (2, "two", 2)];
    let tAC: &[(&str,  &str,  i8)]  = &[("a", "one", 1), ("b", "deux", 2), ("mary", "mary", 3)];
    assert!(rAB.is_sorted());
    assert!(sBC.is_sorted());
    assert!(tAC.is_sorted());

    // Let's plan a triangle query!
    let triangle_it =
        (SliceRange::new(rAB, |x| x.0), SliceRange::new(tAC, |x| x.0))
        .map_value(|(rB, tC)| {
            (SliceBy::new(rB, |x| x.1), SliceRange::new(sBC, |x| x.0))
            .map_value(|(r, sC)| {
                (SliceBy::new(sC, |x| x.1), SliceBy::new(tC, |x| x.1))
                .map_value(|(s, t)| r.2 * s.2 * t.2)
            })
        });

    // Flatten it back to a sorted vector.
    let mut vs: Vec<(&str, usize, &str, i8)> = Vec::new();
    for (a, bcs) in triangle_it.iter() {
        for (b, cs) in bcs.iter() {
            for (c, anno) in cs.iter() {
                vs.push((a, b, c, anno))
            }
        }
    }
    println!("vs: {vs:?}");
    assert!(vs.is_sorted());
}

// EXAMPLE 4: NATIVE ITERATION
#[allow(non_snake_case)]
fn example4() {
    let rAB: &[(&str,  usize, i8)] = &[("a", 1, 1), ("a", 2, 2), ("b", 1, 1), ("b", 2, 2)];
    let sBC: &[(usize, &str,  i8)] = &[(1, "one", 1), (1, "wun", 1), (2, "deux", 2), (2, "two", 2)];
    let tAC: &[(&str,  &str,  i8)]  = &[("a", "one", 1), ("b", "deux", 2), ("mary", "mary", 3)];
    assert!(rAB.is_sorted());
    assert!(sBC.is_sorted());
    assert!(tAC.is_sorted());

    // Triangle query into a sorted vector.
    let mut vs: Vec<(&str, usize, &str, i8)> = Vec::new();
    let rt = SliceRange::new(rAB, |x| x.0).join(SliceRange::new(tAC, |x| x.0));
    for (a, (rB, tC)) in rt.iter() {
        let rs = SliceBy::new(rB, |x| x.1).join(SliceRange::new(sBC, |x| x.0));
        for (b, (r, sC)) in rs.iter() {
            let st = SliceBy::new(sC, |x| x.1).join(SliceBy::new(tC, |x| x.1));
            for (c, (s, t)) in st.iter() {
                vs.push((a, b, c, r.2 * s.2 * t.2))
            }
        }
    }
    println!("vs: {vs:?}");
    assert!(vs.is_sorted());
}

fn main() {
    println!("\n----- EXAMPLE 1 -----");
    example1();

    println!("\n----- EXAMPLE 2 -----");
    example2();

    println!("\n----- EXAMPLE 3 -----");
    example3();

    println!("\n----- EXAMPLE 4 -----");
    example4();
}
