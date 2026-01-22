// Adjustment to GRASPAN.RS, incorporating insights from TRANS2.RS.
#![allow(unused_imports, unused_variables)]

use std::io::prelude::*;
use std::time::Instant;

use dijkstralog::iter::{Seek, ranges, tuples};
use dijkstralog::lsm::{LSM, Layer, Key};

fn main() {
    let mut e = Vec::<(u32, u32)>::new();
    let mut n = Vec::<Key<(u32, u32)>>::new();

    let reading = Instant::now();
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
    println!("Read input in {:.2}s", reading.elapsed().as_secs_f32());

    print!("Sorting e & n... ");
    let sorting = Instant::now();
    std::io::stdout().flush().unwrap();
    e.sort_unstable();
    n.sort_unstable_by_key(|x| x.key);
    println!("done in {:.2}s.", sorting.elapsed().as_secs_f32());
    let e: &[(u32, u32)] = e.as_slice();

    // n a c <- n a b, e b c.
    //
    // implemented as:
    //
    // Δn 0 = n
    // Δn (i+1) a c = Δn i a b, e b c.

    let deducing = Instant::now();
    let mut delta_n: Layer<Key<(u32, u32)>> = Layer::from_sorted(n);
    let mut n: LSM<Key<(u32, u32)>> = LSM::new();

    let mut threshold: usize = 0;

    let mut itercount = 0;
    while !delta_n.is_empty() {
        let iteration = Instant::now();
        itercount += 1;
        println!("\n-- iter {itercount} --");
        let n_size = n.layers().map(|l| l.len()).sum::<usize>();
        let d_size = delta_n.len();
        println!("n (overcount): {n_size:>10} ≈ {n_size:3.0e}");
        println!("      delta_n: {d_size:>10} ≈ {d_size:3.0e}");

        // -- COMPUTE DELTA:  Δn' a c = Δn a b * e b c
        print!("Delta rules ");
        std::io::stdout().flush().unwrap();
        let delta_rules = Instant::now();
        let mut next: Vec<Key<(u32, u32)>> = Vec::new();
        for (a, delta_n_a) in ranges(delta_n.as_slice(), |x| x.key.0).iter() {
            let i = next.len();
            dijkstralog::nest! {
                for (_b, (_delta_n_ab, e_b)) in tuples(delta_n_a, |x| x.key.1)
                    .join(ranges(e, |x| x.0))
                    .iter();
                for &(_, c) in e_b;
                next.push((a, c).into())
            }
            // We're projecting away b. So we sort the remainder (in this case, c) to keep
            // the output in sorted order. Sorting each group separately is faster than
            // doing one big sort at the end.
            next[i..].sort_unstable_by_key(|x| x.key.1);
        }
        let nfound = next.len();
        println!("found {} ≈ {:.0e} potential paths in {:.3}ms.",
                 nfound, nfound, delta_rules.elapsed().as_micros() as f64 / 1000.0);

        // --  UPDATE:  n' = n + Δn
        n.push(delta_n);

        // Times for lnx_kernel_df workload (on battery power) per minification thresholds
        // 500k     ~13.8s
        // 1M       ~13.1s          BEST
        // 2M       ~13.1s
        // 10M      ~16.5s
        //
        // where n = n.layers().map(|l| l.len()).sum();
        // n/50     ~13.7
        // n/30     ~13.5s
        // n/20     ~13.6s
        // n/10     ~14.3s
        // n/5      ~15.7s
        if next.len() < threshold {
            threshold -= next.len();
            next.dedup();
        } else {
            threshold = next.len(); // why does this work well? is this a fluke?
            // next.len() * 2 makes linux & psql faster but http_df slower.
            let n_pre = next.len();
            println!("Deduplicating and minifying...");
            let minify = Instant::now();
            let mut n_seek = n.seeker();
            let mut prev = None;
            next.retain(|ac| {
                // For semiring semantics... I'm not sure what we'd do here.
                if prev.is_some_and(|x| x == ac.key) { return false }
                prev = Some(ac.key);
                // For semiring semantics, here we'd use a minifying operator followed by an
                // is_zero test.
                return n_seek.seek_to(ac.key).is_none()
            });
            let n_post = next.len();
            let dropped = n_pre - n_post;
            println!("{:.3}ms to minify, {n_pre:.1e} → {n_post:.1e}, dropping {:.0}%.",
                     minify.elapsed().as_micros() as f64 / 1000.0,
                     100.0 * (dropped as f32 / n_pre as f32));
        }

        delta_n = Layer::from_sorted(next);
        println!("{:.3}ms total. Time for a new iteration.",
                 iteration.elapsed().as_micros() as f64 / 1000.0);
    }

    let size = n.layers().map(|l| l.len()).sum::<usize>();
    println!("Found {} ≈ {:.0e} possible paths, compressing to deduplicate.", size, size);
    let compressing = Instant::now();
    n.compress();
    println!("Compressing took {:.2}s.", compressing.elapsed().as_secs_f32());

    println!();
    // The sum of layer sizes should be precisely the # of paths, since we
    // minify our deltas.
    let size = n.layers().map(|l| l.len()).sum::<usize>();
    let deducing_secs = deducing.elapsed().as_secs_f32();
    println!("Fixed point reached in {deducing_secs:.2}s. {} ≈ {:.0e} paths in LSM:",
             size, size);
    n.debug_dump(" ");
}
