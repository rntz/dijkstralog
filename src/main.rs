#![allow(dead_code, unused_variables)]

mod lftj;
mod utils;

use lftj::*;

fn main() {
    println!("Hello, world!");

    let primes: Vec<usize> = vec![2, 3, 5, 7, 11];
    let odds = vec![1, 3, 5, 7, 9, 11];
    assert!(primes.is_sorted());
    assert!(odds.is_sorted());

    let mut iter_primes = SliceSeek::new(primes.as_slice());
    let mut iter_odd = SliceSeek::new(odds.as_slice());

    let x: &mut dyn Seek<Item = &usize> = &mut iter_primes;
    let y: &mut dyn Seek<Item = &usize> = &mut iter_odd;

    let lf = Leapfrog::new(vec![x, y]);
    // print some odd primes
    for x in lf {
        println!("{x}");
    }
}
