// Finds, for each vertex v, the minimum-labelled vertex u reachable from v.

use std::io::prelude::*;
use std::time::Instant;

use rayon::prelude::*;          // for parallelism

use dijkstralog::iter::{Seek, ranges, tuples};
use dijkstralog::lsm::{LSM, Layer, Pair, Add};

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

// We use u32::MAX to indicate "infinite weight".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Min(u32);

impl Add for Min {
    fn zero() -> Self { Min(u32::MAX) }
    fn plus(self, other: Self) -> Self { Min(self.0.min(other.0)) }
}

impl From<u32> for Min {
    fn from(x: u32) -> Self { Min(x) }
}

fn main() {
    let load = Instant::now();
    let edges: Vec<(u32, u32)> = load_edges();
    let edges = edges.as_slice();
    let load_ns = load.elapsed().as_nanos();

    let precompute = Instant::now();
    let mut rev_edges: Vec<(u32, u32)> = edges
        .iter()
        .map(|&(a, b)| (b,a))
        .collect();
    rev_edges.par_sort_unstable();
    let precompute_ns = precompute.elapsed().as_nanos();

    // IN LOGIC/CONSTRAINT NOTATION
    //
    //   weight a ≤ b  ←  edge a b.
    //   weight a ≤ c  ←  edge a b  and  weight b = c.

    // IN SEMIRING NOTATION
    //
    //   weight a = Σ_b([edge a b] * b + [edge a b] * weight b)
    //
    // where [p] = 1 if p is true, 0 otherwise
    // and remember, 0 is ∞ and 1 is 0 and + is min and * is plus.

    // IN FSLANG NOTATION
    //
    //   weight a = minimum λb. when (edge a b) (b `min` weight b)
    //
    // why is this f.s. in a??? it's NOT because of interleaving of ⊸ and -fs→! ARGH!

    // IMPLEMENTED AS
    //
    // Δweight 0     a = min (b : edge a b)         = min λb. b          when  edge a b
    // Δweight (i+1) a = min (Δweight b : edge a b) = min λb. Δweight b  when  edge a b

    let compute = Instant::now();
    let mut weight: LSM<Pair<u32, Min>> = LSM::new();
    let mut delta_weight: Vec<Pair<u32, Min>> = edges.iter().map(|&kv| kv.into()).collect();
    debug_assert!(delta_weight.is_sorted_by_key(|x| (x.key, x.value)));

    let mut itercount = 0;
    while !delta_weight.is_empty() {
        itercount += 1;
        println!("\n-- iter {itercount} --");

        // -- DEDUPLICATE & MINIFY DELTA
        println!("Minifying and aggregating delta...");
        {
            let mut weight_seek = weight.seeker();
            let mut prev: Option<Pair<u32, Min>> = None;
            let mut n_dup: usize = 0;
            delta_weight.retain(|&node_weight @ Pair { key: node, value: weight }| {
                if let Some(Pair { key: prev_node, value: prev_weight }) = prev {
                    if node == prev_node {
                        debug_assert!(prev_weight <= weight);
                        n_dup += 1;
                        return false;
                    }
                }
                prev = Some(node_weight);
                return !weight_seek.seek_to(node).is_some_and(|x| x <= weight)
            });
            println!("{n_dup} duplicates found.");
        }

        let nweight = weight.layers().map(|l| l.len()).sum::<usize>();
        let ndelta = delta_weight.len();
        println!("weight        {nweight:>10} ≈ {nweight:3.0e}");
        println!("delta_weight  {ndelta:>10} ≈ {ndelta:3.0e}");

        // -- COMPUTE NEXT DELTA:  Δweight' a = min (Δweight b : edge a b)
        print_flush!("Delta rules ");
        let mut new_weights: Vec<Pair<u32, Min>> = Vec::new();
        for (_b, (&Pair { key: _, value: weight }, rev_edges_b)) in
            tuples(&delta_weight[..], |t| t.key)
            .join(ranges(&rev_edges[..], |x| x.0))
            .iter()
        {
            // Why am I using a vector instead of a hashtable? try the hashtable
            // way. is it any slower? what if I parallelize? can I parallelize
            // the hashtable approach without losing my mind?
            for &(_b, a) in rev_edges_b {
                let q: Pair<u32, Min> = (a, weight).into();
                new_weights.push(q)
            }
        }

        let nweights = new_weights.len();
        println!("found {} ≈ {:.0e} potential new weights.", nweights, nweights);

        // We care about sorting by the weights so that the
        // minification-aggregation step, above, can assume it hits the minimum
        // weight first.
        new_weights.par_sort_unstable_by_key(|x| (x.key, x.value.0));
        debug_assert!(new_weights.is_sorted_by_key(|x| x.key));

        // UPDATE WEIGHTS: weight' = weight + Δweight.
        weight.push(Layer::from_sorted(delta_weight));
        delta_weight = new_weights;
    }

    let compute_ns = compute.elapsed().as_nanos();

    // TODO: some more interesting output here.
    // TODO: check we got the "right" answer?
    println!();
    let size = weight.layers().map(|l| l.len()).sum::<usize>();
    println!("Fixed point reached. {} ≈ {:.0e} entries in LSM:", size, size);
    weight.debug_dump(" ");

    let compress = Instant::now();
    // print_flush!("Counting distinct weights... ");
    // let nweights = weight.iter().count();
    println!("Compressing...");
    let weight = weight.into_layer(); // WHY IS THIS ~4x FASTER? 35ms vs 135ms! on soc-LiveJournal1
    let nweights = weight.len();
    let compress_ns = compress.elapsed().as_nanos();
    println!("found {nweights} ≈ {nweights:.0e} total weights.");

    // println!();
    // println!("  bucketing {:6}ms", total_bucketing_ns / 1_000_000);
    // println!("delta rules {:6}ms", total_delta_rules_ns / 1_000_000);
    // println!(" lsm update {:6}ms", total_lsm_update_ns / 1_000_000);
    // println!("     minify {:6}ms", total_minify_ns / 1_000_000);
    // println!("concatenate {:6}ms", total_concatenate_ns / 1_000_000);
    println!();
    println!("       load {:6}ms", load_ns / 1_000_000);
    println!(" precompute {:6}ms", precompute_ns / 1_000_000);
    println!("    compute {:6}ms", compute_ns / 1_000_000);
    println!("   compress {:6}ms", compress_ns / 1_000_000);
}
