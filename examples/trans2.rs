// DIFF FROM TRANS.RS
//
// We do a projection, Q(a,c) <- ∃b. Δtrans(a,b) edge(b,c). The rhs generates (a,b,c)
// tuples in sorted order. Projecting away b means the (a,c) tuples are not sorted.
// However, they're still sorted by `a`, just each `a`-chunk needs sorting. In trans.rs we
// do this either by sorting the entire results, or a post-processing step that identifies
// the `a`-chunks by searching for them.
//
// Here, we sort each chunk as we generate it, during the projection step, taking
// advantage of our variable-at-a-time nested loops.

// Use eg the SNAP soc-LiveJournal1.txt data set:
// https://snap.stanford.edu/data/soc-LiveJournal1.html
// https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz
// total edges in soc-LiveJournal1.txt: 68,993,773

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
        print!("Delta rules ");
        std::io::stdout().flush().unwrap();
        let mut new_paths: Vec<Key<(u32, u32)>> = Vec::new();
        for (a, delta_a) in ranges(delta_trans.as_slice(), |x| x.key.0).iter() {
            let i = new_paths.len();
            dijkstralog::nest! {
                for (_b, (_delta_ab, edges_b)) in tuples(delta_a, |x| x.key.1)
                    .join(ranges(edges, |x| x.0))
                    .iter();
                for &(_, c) in edges_b;
                // TODO: What if I eliminating existing edges _here_, instead?
                new_paths.push((a, c).into())
            }
            // We're projecting away b. So we sort by the remainder (in this case, c) to
            // ensure our output ends up in sorted order. Sorting each group separately is
            // more efficient than doing one big sort at the end.
            new_paths[i..].sort_by_key(|x| x.key.1);
        }
        let npaths = new_paths.len();
        println!("found {} ≈ {:.0e} potential paths.", npaths, npaths);
        debug_assert!(new_paths.is_sorted_by_key(|x| x.key));

        // --  UPDATE TRANS:  trans' = trans + Δtrans
        trans.push(delta_trans);
        // This^ consumes delta_trans! implications for evaluation order of full
        // Datalog system?

        println!("Deduplicating and minifying...");
        let mut trans_seek = trans.seeker();
        let mut prev = None;
        new_paths.retain(|ac| {
            // For semiring semantics... I'm not sure what we'd do here.
            if prev.is_some_and(|x| x == ac.key) { return false }
            prev = Some(ac.key);
            // For semiring semantics, here we'd use a minifying operator followed by an
            // is_zero test.
            return trans_seek.seek_to(ac.key).is_none()
        });

        println!("Minified. Time for a new iteration.");
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
