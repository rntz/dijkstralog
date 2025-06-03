#![allow(dead_code, unused_variables, unused_mut, unused_imports)]

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
    SliceRangeSeek,
    Seek,
};

fn main() {
    println!("hello world!");

    let xs: &[(isize, &str)] = &[(1, "one"), (1, "wun"), (2, "two"), (2, "deux")];
    let mut it = SliceRangeSeek::new(xs, |x| x.0);

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
}
