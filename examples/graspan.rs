#![allow(unused_imports, unused_variables)]

use std::io::prelude::*;

use dijkstralog::iter::{Seek, ranges, tuples};
use dijkstralog::lsm::{LSM, Layer, Key};

const DEBUG: bool = false;

fn main() {
    let mut e = Vec::<(u32, u32)>::new();
    let mut n = Vec::<Key<(u32, u32)>>::new();

    for filename in std::env::args().skip(1) {
        use std::fs::File;
        use std::io::{BufRead, BufReader};
        println!("Reading {filename}");
        let file = BufReader::new(File::open(filename).unwrap());
        for readline in file.lines() {
            let line = readline.expect("read error");
            if line.is_empty() { continue }
            if line.starts_with('#') { continue }
            let mut elts = line.split_whitespace().rev();
            let Some(name) = elts.next() else { continue };
            let mut elts = elts.rev();
            let x = elts.next().unwrap().parse().expect("malformed x");
            let y = elts.next().unwrap().parse().expect("malformed y");
            if name == "n" { n.push((x, y).into()) }
            else if name == "e" { e.push((x, y)) }
        }
        println!("e: {}\nn: {}", e.len(), n.len());
    }

    print!("Sorting e & n... ");
    std::io::stdout().flush().unwrap();
    e.sort_unstable();
    n.sort_unstable_by_key(|x| x.key);
    println!("done.");
    let e: &[(u32, u32)] = e.as_slice();

    // n a c <- n a b, e b c.
    //
    // implemented as:
    //
    // Δn 0 = n
    // Δn (i+1) a c = Δn i a b, e b c.

    let mut delta_n: Layer<Key<(u32, u32)>> = Layer::from_sorted(n);
    let mut n: LSM<Key<(u32, u32)>> = LSM::new();

    let mut itercount = 0;
    while !delta_n.is_empty() {
        itercount += 1;
        println!("\n-- iter {itercount} --");
        let n_size = n.layers().map(|l| l.len()).sum::<usize>();
        let d_size = delta_n.len();
        println!("n (overcount): {n_size:>10} ≈ {n_size:3.0e}");
        println!("      delta_n: {d_size:>10} ≈ {d_size:3.0e}");

        // -- COMPUTE DELTA:  Δn' a c = Δn a b * e b c
        print!("Delta rules ");
        std::io::stdout().flush().unwrap();
        let mut next: Vec<(u32, u32)> = Vec::new();
        dijkstralog::nest! {
            for (a, delta_n_a) in ranges(delta_n.as_slice(), |x| x.key.0).iter();
            for (_b, (_delta_n_ab, e_b)) in tuples(delta_n_a, |x| x.key.1)
                .join(ranges(e, |x| x.0))
                .iter();
            for &(_, c) in e_b;
            next.push((a, c))
        }
        let nfound = next.len();
        println!("found {} ≈ {:.0e} potential paths.", nfound, nfound);

        // --  UPDATE:  n' = n + Δn
        n.push(delta_n);

        println!("Sorting {} ≈ {:.0e} new paths...", nfound, nfound);
        next.sort_unstable(); // <-- the plurality of our time is spent here
        println!("Sorted, now deduplicating...");
        next.dedup();      // is this slow?
        println!("Deduplicated to {} paths, now minifying...", next.len());
        let mut new_delta_n: Vec<Key<(u32, u32)>> = Vec::with_capacity(next.len());
        let mut n_seek = n.seeker();
        for ac in next {
            if n_seek.seek_to(ac).is_none() {
                if DEBUG { println!("found {:>2} -> {:<2}", ac.0, ac.1); }
                new_delta_n.push(ac.into())
            } else {
                if DEBUG { println!("      {:>2} -> {:<2} already found", ac.0, ac.1);
                }
            }
        }
        println!("Minified. Time for a new iteration.");
        delta_n = Layer::from_sorted(new_delta_n);
    }

    println!();
    // The sum of layer sizes should be precisely the # of paths, since we
    // minify our deltas.
    let size = n.layers().map(|l| l.len()).sum::<usize>();
    println!("Fixed point reached. {} ≈ {:.0e} paths in LSM:", size, size);
    n.debug_dump(" ");
}
