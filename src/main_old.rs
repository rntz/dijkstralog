// #![allow(dead_code, unused_variables)]

mod lftj;
mod utils;

use lftj::*;

fn main() {
    println!("Hello, world!");

    let primes = vec![2, 3, 5, 7, 11];
    let odds = vec![1, 3, 5, 7, 9, 11];
    assert!(primes.is_sorted());
    assert!(odds.is_sorted());

    let iter_primes: &mut dyn Seek<Item = &usize> = &mut SliceSeek::new(primes.as_slice());
    let iter_odds: &mut dyn Seek<Item = &usize> = &mut SliceSeek::new(odds.as_slice());
    let lf = Leapfrog::new(vec![iter_primes, iter_odds]);

    // print some odd primes
    for x in lf {
        println!("{x}");
    }
}
