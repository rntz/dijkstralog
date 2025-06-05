#![allow(dead_code, unused_imports, unused_variables, unused_mut, unused_macros)]

mod iter;
mod temp;

use iter::{
    Position, Position::{*},
    Bound, Bound::{*},
    Seek,
    ranges, tuples,
};

// EXAMPLE 1: TRIES
fn example1() {
    let xys: &[(isize, &str)] = &[(1, "one"), (1, "wun"), (2, "two"), (2, "deux")];

    // Iterate through the slices on the first component of the tuples.
    let mut it = ranges(xys, |t| t.0);
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
        ranges(xys, |t| t.0)
        .map(|ys| tuples(ys, |t| t.1).keys().collect())
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
        ranges(r, |t| t.0).map(|bs| ranges(bs, |t| t.1).map(|_| 2));
    let mut s_bc =
        ranges(s, |t| t.0).map(|cs| ranges(cs, |t| t.1).map(|_| 3));
    let mut t_ac =
        ranges(t, |t| t.0).map(|cs| ranges(cs, |t| t.1).map(|_| 5));

    let rtrie: Vec<(_, Vec<_>)> = r_ab.clone().map(|bs| bs.collect()).collect();
    println!("rtrie: {rtrie:?}");

    // Let's plan a triangle query!
    // this requires we pay obeisance to the borrow checking gods.
    // I worry about what the closures look like...
    // I should try disassembly, but it might be too big to understand.
    let triangle_it =
        r_ab.join(t_ac).map(move |(r_b, t_c)| {
            r_b.join(s_bc.clone()).map(move |(r, s_c)| {
                s_c.join(t_c.clone()).map(move |(s, t)| r * s * t)
            })
        });

    let triangles: Vec<(&str, Vec<(isize, Vec<(&str, isize)>)>)> =
        triangle_it.map(|bcs| bcs.map(|cs| cs.collect()).collect())
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
        ranges(rAB, |x| x.0)
        .join(ranges(tAC, |x| x.0))
        .map(|(rB, tC)| {
            tuples(rB, |x| x.1)
            .join(ranges(sBC, |x| x.0))
            .map(|(r, sC)| {
                tuples(sC, |x| x.1)
                .join(tuples(tC, |x| x.1))
                .map(|(s, t)| r.2 * s.2 * t.2)
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
    let rt = ranges(rAB, |x| x.0).join(ranges(tAC, |x| x.0));
    for (a, (rB, tC)) in rt.iter() {
        let rs = tuples(rB, |x| x.1).join(ranges(sBC, |x| x.0));
        for (b, (r, sC)) in rs.iter() {
            let st = tuples(sC, |x| x.1).join(tuples(tC, |x| x.1));
            for (c, (s, t)) in st.iter() {
                vs.push((a, b, c, r.2 * s.2 * t.2))
            }
        }
    }

    println!("vs: {vs:?}");
    assert!(vs.is_sorted());
}

// EXAMPLE 5: LOOKUPS
#[allow(non_snake_case)]
fn example5() {
    let xs: &[(isize, &str)] = &[(17, "hello"), (17, "goodbye"), (23, "hello"), (27, "goodbye")];
    let xs_it = ranges(xs, |x| x.0);
    let point = xs_it.clone().lookup(17);

    let rAB: &[(&str,  usize, i8)] = &[("a", 1, 1), ("a", 2, 2), ("b", 1, 1), ("b", 2, 2)];
    let sBC: &[(usize, &str,  i8)] = &[(1, "one", 1), (1, "wun", 1), (2, "deux", 2), (2, "two", 2)];
    let tAXC: &[(&str,  i32, &str,  i8)] =
        &[("a", 17, "one", 1), ("b", 17, "deux", 2), ("mary", 17, "mary", 3)];
    assert!(rAB.is_sorted());
    assert!(sBC.is_sorted());
    assert!(tAXC.is_sorted());

    // Triangle query into a sorted vector.
    let mut vs: Vec<(&str, usize, &str, i8)> = Vec::new();

    let r_ab = ranges(rAB, |x| x.0);
    let s_bc = ranges(sBC, |x| x.0);
    let t_axc = ranges(tAXC, |x| x.0);
    for (a, (rB, tXC)) in r_ab.join(t_axc).iter() {
        let r_b = tuples(rB, |x| x.1);
        let t_c = match ranges(tXC, |x| x.1).lookup(17)
                  { Some(s) => tuples(s, |x| x.2), None => continue, };
        for (b, (r, sC)) in r_b.join(s_bc.clone()).iter() {
            let s_c = tuples(sC, |x| x.1);
            for (c, (s, t)) in s_c.join(t_c.clone()).iter() {
                vs.push((a, b, c, r.2 * s.2 * t.3))
            }
        }
    }

    println!("vs: {vs:?}");
    assert!(vs.is_sorted());
}

fn main() {
    println!("\n---------- temp::main() ----------");
    temp::main();

    println!("\n----- EXAMPLE 1 -----");
    example1();

    println!("\n----- EXAMPLE 2 -----");
    example2();

    println!("\n----- EXAMPLE 3 -----");
    example3();

    println!("\n----- EXAMPLE 4 -----");
    example4();

    println!("\n----- EXAMPLE 5 -----");
    example5();
}
