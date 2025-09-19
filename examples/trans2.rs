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
//
// HOWEVER, this is harder to parallelize! If I just replace these smaller sorts with
// Rayon's par_sort_unstable, I get less speedup than doing the same in trans.rs, because
// I've *sequentialized* all these little sorts. As a result, parallelizing trans.rs is
// often faster than parallel trans2.rs (on my Macbook M1 Pro).

// Works with various SNAP datasets, eg wiki-Vote.txt or soc-LiveJournal1.txt.
// https://snap.stanford.edu/data/index.html
use std::io::prelude::*;

use dijkstralog::iter::{Seek, ranges, tuples};
use dijkstralog::lsm::{LSM, Layer, Key};

// Set EDGES environment variable to override; EDGES=all for no limit.
const DEFAULT_MAX_EDGES: usize = 250_000;
const DEFAULT_FILE: &str = "data/ca-HepPh.txt";

macro_rules! print_flush {
    ($($e:tt)*) => { { print!($($e)*); std::io::stdout().flush().unwrap() } }
}

fn load_edges() -> Vec<(u32, u32)> {
    use std::ffi::OsString;
    use std::fs::File;
    use std::env::{var, VarError};
    let args = std::env::args_os();
    let path: OsString = args.skip(1).next().unwrap_or(DEFAULT_FILE.into());
    let file = File::open(&path).expect("couldn't open file");
    println!("Reading from {:?}", path);
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
        let mut new_paths: Vec<Key<(u32, u32)>> = Vec::new();
        for (a, delta_a) in ranges(delta_trans.as_slice(), |x| x.key.0).iter() {
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
            // PARALLEL SORT
            // use rayon::prelude::*;
            // new_paths[i..].par_sort_unstable_by_key(|x| x.key.1);
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
        let n_pre = new_paths.len();
        let mut n_dup: usize = 0;
        new_paths.retain(|ac| {
            // For semiring semantics... I'm not sure what we'd do here.
            if prev.is_some_and(|x| x == ac.key) { n_dup += 1; return false }
            prev = Some(ac.key);
            // For semiring semantics, here we'd use a minifying operator followed by an
            // is_zero test.
            return trans_seek.seek_to(ac.key).is_none()
        });
        let n_post = new_paths.len();
        let n_known = n_pre - n_post - n_dup;
        println!(
            "Minified {n_pre:.1e} → {n_post:.1e}, new {:.0}%, dups {:.0}%, known {:.0}%",
            100.0 * (n_post as f32 / n_pre as f32),
            100.0 * (n_dup as f32 / n_pre as f32),
            100.0 * (n_known as f32 / n_pre as f32),
        );

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
