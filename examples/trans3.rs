// DIFF FROM TRANS2.RS:
//
// Instead of minifying the materialized Δtrans, we minify by excluding entries from
// `trans` when we join with `Δtrans` in the delta rules.
//
// This seemed faster than trans2 on soc-LiveJournal1. However, it's slower on wiki-Vote.
// I think the speedup may be workload-dependent and/or an artifact of the fact that we
// only use a subset of the edges of soc-LiveJournal1 when testing (the full dataset is
// too large).

use std::io::prelude::*;

use dijkstralog::iter::{Seek, ranges, tuples};
use dijkstralog::lsm::{LSM, Layer, Key};

// Takes ≤ 3s on my Macbook M1 Pro.
// Set EDGES environment variable to override; EDGES=all for no limit.
const DEFAULT_MAX_EDGES: usize = 110_000;

macro_rules! print_flush {
    ($($e:tt)*) => { { print!($($e)*); std::io::stdout().flush().unwrap() } }
}

fn load_edges() -> Vec<(u32, u32)> {
    // TODO: use first std::env::args as data file if present
    use std::fs::File;
    use std::path::Path;
    let path = Path::new("data/wiki-Vote.txt");
    let file = File::open(&path).expect("couldn't open wiki-Vote.txt");
    // let path = Path::new("data/soc-LiveJournal1.txt");
    // let file = File::open(&path).expect("couldn't open soc-LiveJournal1.txt");
    use std::env::{var, VarError};
    let max_edges: Option<usize> = match var("EDGES") {
        Err(VarError::NotPresent) => Some(DEFAULT_MAX_EDGES), // default
        Err(VarError::NotUnicode(_)) => panic!("EDGES not valid unicode"),
        // explicit ways to set "no limit"
        Ok(s) if s == "all" => None,
        Ok(s) => Some({
            let (factor, s) = if let Some(t) = s.strip_suffix("k") {
                (1_000, t)
            } else if let Some(t) = s.strip_suffix("M") {
                (1_000_000, t)
            } else if let Some(t) = s.strip_suffix("m") {
                (1_000_000, t)
            } else {
                (1, &s[..])
            };
            factor * s.parse::<usize>().expect("malformed MAX_EDGES")
        }),
    };
    return load_edges_from(file, max_edges)
}

fn load_edges_from<R: std::io::Read>(source: R, max_edges: Option<usize>) -> Vec<(u32, u32)> {
    if let Some(n) = max_edges {
        println!("Reading at most {n} edges...");
    } else {
        println!("Reading all edges...");
    }
    use std::io::{BufRead, BufReader};
    let file = BufReader::new(source);
    let mut edges: Vec<(u32, u32)> = Vec::new();
    for readline in file.lines() {
        if max_edges.is_some_and(|n| n <= edges.len()) { break }
        let line = readline.expect("read error");
        if line.is_empty() { continue }
        if line.starts_with('#') { continue }
        let mut elts = line[..].split_whitespace();
        let v: u32 = elts.next().unwrap().parse().expect("malformed src");
        let u: u32 = elts.next().unwrap().parse().expect("malformed dst");
        edges.push((v,u));
    }
    print_flush!("{} edges", edges.len());
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
    let edges: Vec<(u32, u32)> = load_edges();
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
        print_flush!("Delta rules ");
        let mut new_paths: Vec<Key<(u32, u32)>> = Vec::new();
        let mut group_a: Vec<Key<(u32, u32)>> = Vec::new();
        {
            let mut trans_seek = trans.seeker();
            for (a, delta_a) in ranges(delta_trans.as_slice(), |x| x.key.0).iter() {
                dijkstralog::nest! {
                    for (b, (_delta_ab, edges_b)) in tuples(delta_a, |x| x.key.1)
                        .join(ranges(edges, |x| x.0))
                        .iter();
                    // We didn't minify delta_trans at materialization time, so we do it
                    // now by excluding existing paths.
                    if let None = trans_seek.seek_to((a, b));
                    for &(_, c) in edges_b;
                    group_a.push((a, c).into())
                }
                // We're projecting away b. So we sort by the remainder (in this case, c) to
                // ensure our output ends up in sorted order. Sorting each group separately is
                // more efficient than doing one big sort at the end.
                group_a.sort_by_key(|x| x.key.1);
                group_a.dedup();
                new_paths.append(&mut group_a);
            }
        }
        let npaths = new_paths.len();
        println!("found {} ≈ {:.0e} new paths.", npaths, npaths);
        debug_assert!(new_paths.is_sorted_by_key(|x| x.key));

        // --  UPDATE TRANS:  trans' = trans + Δtrans
        trans.push(delta_trans);
        // This^ consumes delta_trans! implications for evaluation order of full
        // Datalog system?

        delta_trans = Layer::from_sorted(new_paths);
    }

    println!();
    // The sum of layer sizes over-estimates the # of paths, since we only minify our
    // deltas lazily (when joining them to compute the next iteration).
    let size = trans.layers().map(|l| l.len()).sum::<usize>();
    println!("Fixed point reached. {} ≈ {:.0e} paths in LSM:", size, size);
    trans.debug_dump(" ");

    // WHY IS THIS SO MUCH FASTER THAN COUNTING?!?!?! WTF?!?!
    println!("Compressing...");
    let trans = trans.into_layer();
    let npaths = trans.len();
    println!("found {npaths} ≈ {npaths:.0e} total paths.");

    // print_flush!("Counting distinct paths... ");
    // let npaths = trans.iter().count();
    // println!("found {npaths} ≈ {:.0e}", npaths);

    // // Comment the rest out if you don't care about just getting one big vector.
    // print_flush!("Merging all paths into one vector... ");
    // let paths: Vec<(u32, u32)> = trans.iter().map(|x| x.0).collect();
    // println!("done.");
    // println!("Found {} = {:.2e} paths", paths.len(), paths.len());
    // if DEBUG {
    //     println!("paths: {:?}", paths.as_slice());
    // }

}
