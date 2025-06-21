// Use the SNAP soc-LiveJournal1.txt data set:
// https://snap.stanford.edu/data/soc-LiveJournal1.html
// https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz

use dijkstralog::iter::{Seek, ranges, tuples};
use dijkstralog::lsm::{LSM, Layer, Key};

use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;

// total edges in soc-LiveJournal1.txt: 68,993,773
//const MAX_EDGES: usize = 48;
//const MAX_EDGES: usize = 20_000;
const MAX_EDGES: usize = 80_000;
//const MAX_EDGES: usize = 100_000;
//const MAX_EDGES: usize = 150_000;
//const MAX_EDGES: usize = 250_000; // ~1min under --release
//const MAX_EDGES: usize = 1_000_000; // don't do this.

fn load_livejournal() -> Vec<(u32, u32)> {
    let path = Path::new("data/soc-LiveJournal1.txt");
    let file = BufReader::new(File::open(&path).expect("couldn't open soc-LiveJournal1.txt"));

    let mut edges: Vec<(u32, u32)> = Vec::new();

    println!("Reading edges from soc-LiveJournal1.txt...");
    let mut n = 0;
    for readline in file.lines() {
        let line = readline.expect("read error");
        if line.is_empty() { continue; }
        if line.starts_with('#') { continue; }

        if MAX_EDGES <= n { break }
        n += 1;

        let mut elts = line[..].split_whitespace();
        let v: u32 = elts.next().unwrap().parse().expect("malformed src");
        let u: u32 = elts.next().unwrap().parse().expect("malformed dst");
        edges.push((v,u));
    }

    let n_edges = edges.len();
    print!("{n_edges} edges");
    std::io::stdout().flush().unwrap();
    if edges.is_sorted() {
        println!(", already sorted");
    } else {
        println!(", sorting...");
        edges.sort();
        println!("sorted!");
    }

    return edges;
}

const DEBUG: bool = false;

fn main() {
    let edges: Vec<(u32, u32)> = load_livejournal();
    let edges = edges.as_slice();
    if DEBUG {
        println!("edges: {:?}", edges);
    }

    // trans a b <- edge a b
    // trans a c <- trans a b, edge b c
    //
    // implemented as
    //
    // Δtrans 0     a b = edge a b
    // Δtrans (i+1) a c = Δtrans i a b * edge b c
    //  trans (i+1)     = trans i + Δtrans i

    let mut trans: LSM<Key<(u32, u32)>> = LSM::new();
    let mut delta_trans: Layer<Key<(u32, u32)>> = Layer::from_sorted(
        edges.iter().map(|&kv| kv.into()).collect()
    );

    let mut itercount = 0;
    while !delta_trans.as_slice().is_empty() {
        itercount += 1;
        println!("\n-- iter {itercount} --");
        let ntrans = trans.layers().map(|l| l.len()).sum::<usize>();
        let ndelta = delta_trans.len();
        println!("trans (overcount): {ntrans:>10} ≈ {ntrans:3.0e}");
        println!("      delta_trans: {ndelta:>10} ≈ {ndelta:3.0e}");

        // -- COMPUTE DELTA:  Δtrans' a c = Δtrans a b * edge b c
        print!("Delta rules ");
        std::io::stdout().flush().unwrap();
        let mut new_paths: Vec<(u32, u32)> = Vec::new();
        dijkstralog::nest! {
            for (a, delta_a) in ranges(delta_trans.as_slice(), |x| x.key.0).iter();
            for (_b, (_delta_ab, edges_b)) in tuples(delta_a, |x| x.key.1)
                .join(ranges(edges, |x| x.0))
                .iter();
            for &(_, c) in edges_b;
            new_paths.push((a, c))
        }
        let npaths = new_paths.len();
        println!("found {} ≈ {:.0e} potential paths.", npaths, npaths);

        // --  UPDATE TRANS:  trans' = trans + Δtrans
        trans.push(delta_trans);
        // This^ consumes delta_trans! implications for evaluation order of full
        // Datalog system?

        // This is a trick that it would be slightly annoying to get a Datalog
        // compiler to do.
        #[allow(dead_code)]
        fn sort_new_paths(new_paths: &mut [(u32, u32)]) {
            debug_assert!(new_paths.is_sorted_by_key(|x| x.0));
            let mut i = 0;
            while i < new_paths.len() {
                let (a, _) = new_paths[i];
                // Could use galloping search here, might be faster (might not!), but the
                // cost is completely dominated by sorting.
                let j = new_paths.partition_point(|x| x.0 <= a);
                debug_assert!(i < j);
                debug_assert!(new_paths[j-1].0 == a);
                new_paths[i..j].sort_by_key(|x| x.1);
                debug_assert!(new_paths[i..j].is_sorted());
                i = j;
            }
        }

        println!("Sorting {} ≈ {:.0e} new paths...", npaths, npaths);
        new_paths.sort();       // <-- THE PLURALITY OF OUR TIME IS SPENT HERE
        // sort_new_paths(new_paths.as_mut_slice()); // measurably faster but not amazingly so
        println!("Sorted, now deduplicating...");
        new_paths.dedup();      // is this slow?
        println!("Deduplicated to {} paths, now minifying...", new_paths.len());
        let mut new_delta_trans: Vec<Key<(u32, u32)>> = Vec::with_capacity(new_paths.len());
        let mut trans_seek = trans.seeker();
        for ac in new_paths {
            // this is taking a significant amount of time!
            if trans_seek.seek_to(ac).is_none() {
                if DEBUG { println!("found {:>2} -> {:<2}", ac.0, ac.1); }
                new_delta_trans.push(ac.into())
            } else {
                if DEBUG { println!("      {:>2} -> {:<2} already found", ac.0, ac.1);
                }
            }
        }
        println!("Minified. Time for a new iteration.");
        delta_trans = Layer::from_sorted(new_delta_trans);
    }

    println!();
    // The sum of layer sizes should be precisely the # of paths, since we
    // minify our deltas.
    let size = trans.layers().map(|l| l.len()).sum::<usize>();
    println!("Fixed point reached. {} ≈ {:.0e} paths in LSM:", size, size);
    trans.debug_dump(" ");

    // // Comment the rest out if you don't care about just getting one big vector.
    // print!("Merging all paths into one vector... ");
    // std::io::stdout().flush().unwrap();
    // let paths: Vec<(u32, u32)> = trans.iter().map(|x| x.0).collect();
    // println!("done.");
    // println!("Found {} = {:.2e} paths", paths.len(), paths.len());
    // if DEBUG {
    //     println!("paths: {:?}", paths.as_slice());
    // }
    // // TODO: debug-print the layers of our LSM.

}
