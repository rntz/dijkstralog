// DIFF FROM TRANS2.RS
//
// This file follows trans2.rs (see there for diff from trans.rs) but also parallelizes
// the entire inner loop by partitioning the delta up to N=32 ways using Rayon's parallel
// iterators. (Future work: don't hard-code the degree of parallelism.)

use std::io::prelude::*;

use rayon::prelude::*;          // for parallelism
// use itertools::Itertools;       // for dedup() on iterators

use dijkstralog::iter::{Seek, ranges, tuples};
use dijkstralog::lsm::{LSM, Layer, Key};

// Takes ≤ 3s on my Macbook M1 Pro.
// Set EDGES environment variable to override; EDGES=all for no limit.
const DEFAULT_MAX_EDGES: usize = 250_000;

macro_rules! print_flush {
    ($($e:tt)*) => { { print!($($e)*); std::io::stdout().flush().unwrap() } }
}

fn load_edges() -> Vec<(u32, u32)> {
    // TODO: use first std::env::args as data file if present
    use std::fs::File;
    use std::path::Path;
    //let path = Path::new("data/wiki-Vote.txt");
    let path = Path::new("data/email-Enron.txt");
    // let path = Path::new("data/soc-Epinions1.txt");
    // let path = Path::new("data/soc-LiveJournal1.txt");
    let file = File::open(&path).expect("couldn't open file");
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
        edges.sort_unstable();
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
    while !delta_trans.is_empty() {
        itercount += 1;
        println!("\n-- iter {itercount} --");
        let ntrans = trans.layers().map(|l| l.len()).sum::<usize>();
        let ndelta = delta_trans.len();
        println!("trans (overcount): {ntrans:>10} ≈ {ntrans:3.0e}");
        println!("      delta_trans: {ndelta:>10} ≈ {ndelta:3.0e}");

        // -- COMPUTE DELTA:  Δtrans' a c = Δtrans a b * edge b c
        print_flush!("Delta rules ");

        // Works on a chunk of the delta.
        let do_things = |delta: &[Key<(u32, u32)>]| {
            let mut new_paths: Vec<Key<(u32, u32)>> = Vec::new();
            for (a, delta_a) in ranges(delta, |x| x.key.0).iter() {
                let i = new_paths.len();
                dijkstralog::nest! {
                    for (_b, (_delta_ab, edges_b)) in tuples(delta_a, |x| x.key.1)
                        .join(ranges(edges, |x| x.0))
                        .iter();
                    for &(_, c) in edges_b;
                    new_paths.push((a, c).into())
                }
                // We're projecting away b. So we sort by the remainder (in this case, c) to
                // ensure our output ends up in sorted order. Sorting each group separately is
                // more efficient than doing one big sort at the end.
                new_paths[i..].sort_unstable_by_key(|x| x.key.1);
            }
            // For semiring semantics, would need to make this aggregate.
            new_paths.dedup();
            return new_paths;
        };

        // How many ways do we want to parallelize?
        let delta = delta_trans.as_slice();
        let ndelta = delta.len();
        // Over-parallelize to reduce impact of "chunkiness" (uneven amounts of
        // work per partition).
        let max_splits: usize = 32;

        let mut partitions: Vec<(usize, usize)> = Vec::new();
        let mut start = 0;
        let mut prev_a = delta_trans[0].key.0;
        for i in 0..max_splits {
            let a = delta[i * (ndelta / max_splits)].key.0;
            if a == prev_a { continue }
            let end = delta.partition_point(|row| row.key.0 <= a);
            assert!(start != end);
            partitions.push((start, end));
            start = end;
            prev_a = a;
        }
        partitions.push((start, ndelta));
        println!("partitioned {} delta tuples into {} buckets:\n  {:?}",
                 ndelta, partitions.len(), &partitions);
        let new_paths: Vec<Vec<_>> = partitions
            .into_par_iter()
            .map(|(start, end)| do_things(&delta[start..end]))
            .collect();

        let n_pre_minify: usize = new_paths.iter().map(|v| v.len()).sum();
        println!("found {} ≈ {:.0e} potential paths.", n_pre_minify, n_pre_minify);

        // --  UPDATE TRANS:  trans' = trans + Δtrans
        trans.push(delta_trans);
        // This^ consumes delta_trans! implications for evaluation order of full
        // Datalog system?

        // Parallel minification. It would be good to fuse this with the generation of
        // new_paths, but we can't because we don't have a persistent way of updating an
        // LSM yet. Need Rc<> on each layer or similar so different LSMs can share Layers.
        println!("Minifying...");
        let new_paths: Vec<Vec<_>> = new_paths.into_par_iter().map(|mut new_paths| {
            let mut trans_seek = trans.seeker();
            new_paths.retain(|ac| {
                // For semiring semantics, here we'd use a minifying operator followed by
                // an is_zero test.
                return trans_seek.seek_to(ac.key).is_none()
            });
            new_paths
        }).collect();
        let n_post_minify: usize = new_paths.iter().map(|v| v.len()).sum();
        println!(
            "Minified {n_pre_minify:.1e} → {n_post_minify:.1e}, new {:.0}%",
            100.0 * (n_post_minify as f32 / n_pre_minify as f32),
        );

        println!("Concatenating...");
        let n_extra = new_paths[1..].iter().map(|v| v.len()).sum();
        let mut new_path_vecs = new_paths.into_iter();
        let mut new_paths = new_path_vecs.next().unwrap();
        new_paths.reserve_exact(n_extra);
        for more_paths in new_path_vecs {
            new_paths.extend(more_paths);
        }

        delta_trans = Layer::from_sorted(new_paths);
    }

    println!();
    // The sum of layer sizes should be precisely the # of paths, since we
    // minify our deltas.
    let size = trans.layers().map(|l| l.len()).sum::<usize>();
    println!("Fixed point reached. {} ≈ {:.0e} paths in LSM:", size, size);
    trans.debug_dump(" ");

    // print_flush!("Counting distinct paths... ");
    // let npaths = trans.iter().count();
    // println!("found {npaths} ≈ {:.0e}", npaths);

    // // Comment the rest out if you don't care about just getting one big vector.
    // print!("Merging all paths into one vector... ");
    // std::io::stdout().flush().unwrap();
    // let paths: Vec<(u32, u32)> = trans.iter().map(|x| x.0).collect();
    // println!("done.");
    // println!("Found {} = {:.2e} paths", paths.len(), paths.len());
    // if DEBUG {
    //     println!("paths: {:?}", paths.as_slice());
    // }

}
