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
    Seek,
};

// EXAMPLE 1: TRIES
fn example1() {
    let xs: &[(isize, &str)] = &[(1, "one"), (1, "wun"), (2, "two"), (2, "deux")];

    // Iterate through the slices on the first component of the tuples.
    let mut it = SliceRange::new(xs, |x| x.0);
    loop {
        let p = it.posn();
        println!();
        println!(" p: {p:?}");
        println!("it: {it:?}");
        match p {
            Know(Done) => break,
            Know(p) => it.seek(&p),
            Have(k, _) => it.seek(&Greater(k)),
        }
    }

    // Dump it into a trie using nested SliceRange iteration.
    println!();
    let trie =
        SliceRange::new(xs, |tuple| tuple.0)
        .collect_with(|x, tuples| {
            let vec =
                SliceRange::new(tuples, |tuple| tuple.1)
                .collect_with(|y, tuples| {
                    assert!(tuples.len() == 1);
                    *y
                });
            (*x, vec)
        });
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

    let mut r_ =
        SliceRange::new(r, |x| x.0)
        .map(|tuples| SliceRange::new(tuples, |x| x.1).map(|slice| 2));
    let mut s_ =
        SliceRange::new(s, |x| x.0)
        .map(|tuples| SliceRange::new(tuples, |x| x.1).map(|slice| 3));
    let mut t_ = SliceRange::new(t, |x| x.0)
        .map(|tuples| SliceRange::new(tuples, |x| x.1).map(|slice| 5));

    let rtrie = r_.clone().collect_with(|a, bit| {
        (*a, bit.collect_with(|b, v| (*b,v)))
    });
    println!("rtrie: {rtrie:?}");

    // Let's plan a triangle query!
    // this requires we pay obeisance to the borrow checking gods.
    // I worry about what the closures look like...
    // I should try disassembly, but it might be too big to understand.
    let triangle_it =
        r_.join(t_)
        .map(move |(r_a, t_a)| {
            r_a.join(s_.clone())
                .map(move |(r_ab, s_b)| {
                    s_b.join(t_a.clone())
                        .map(move |(s_bc, t_ac)| {
                            r_ab * s_bc * t_ac
                        })
                })
        });

    let vs = triangle_it
        .collect_with(|a, tuples| {
            (*a, tuples.collect_with(|b, tuples| {
                (*b, tuples.collect_with(|c, value| (*c,value)))
            }))
        });
    println!("vs: {vs:?}");
}

// EXAMPLE 3: TRIANGLE QUERY WITH LESS CEREMONY
fn example3() {
    let r: &[(&str, isize)] = &[("a", 1), ("a", 2), ("b", 1), ("b", 2)];
    let s: &[(isize, &str)] = &[(1, "one"), (1, "wun"), (2, "deux"), (2, "two")];
    let t: &[(&str, &str)]  = &[("a", "one"), ("b", "deux"), ("mary", "mary")];
    assert!(r.is_sorted());
    assert!(s.is_sorted());
    assert!(t.is_sorted());

    // Let's plan a triangle query!
    let triangle_it =
        SliceRange::new(r, |x| x.0)
        .join(SliceRange::new(t, |x| x.0))
        .map(|(r_a, t_a)| {
            SliceRange::new(r_a, |x| x.1)
                .join(SliceRange::new(s, |x| x.0))
                .map(|(r_ab, s_b)| {
                    SliceRange::new(s_b, |x| x.1)
                        .join(SliceRange::new(t_a, |x| x.1))
                        .map(|_| ())
                })
        });

    let vs = triangle_it
        .collect_with(|a, tuples| {
            (*a, tuples.collect_with(|b, tuples| {
                (*b, tuples.collect_with(|c, ()| *c))
            }))
        });
    println!("vs: {vs:?}");
}

fn main() {
    println!("hello world!");
    example1();
    println!();
    example2();
    println!();
    example3();
}
