// Maintaining 2-edge query
// against triangle-completion rewrite rule.
//
// QUERY    {1,2,3} edge(1,2) edge(2,3)
// REWRITE  {a,c} edge(a,c) → {a,b,c} edge(a,c) edge(a,b) edge(b,c)
//
// STRATEGY
//
// Repeatedly pick an edge(a,c) from the current edge-set at random. The rewrite
// isn't idempotent so there's no point in splitting into "processed"
// edges and "unprocessed" edges to try to hit a fixed point.
//
// STRATEGY FOR QUERY MAINTENANCE
//
// The rewrite can introduce 4 classes of new matches:
//
// 1. ∀ edge(X,a)
//    → edge(X,a) edge(a,b)     is a new 2-length path
//    so look for edges into `a` using a HashMap index.
//
// 2. ∀ edge(c,X)
//    → edge(b,c) edge(c,X)     is a new 2-length path
//    so look for edges out of `c` using a HashMap index.
//
// 3. edge(a,b) edge(b,c) is always a new 2-length path.
// 4. if a = c then edge(b,c) edge(a,b) is a new 2-length path.
//
// So we need two hash-indexes on edge(a,b):
// forward a → b for (2), and reverse b → a for (1).

use std::io::prelude::*;
use std::collections::HashMap;
use std::env::{var, VarError};
use std::str::FromStr;
use std::time::Instant;

// Set EDGES environment variable to override; EDGES=all for no limit.
const DEFAULT_MAX_EDGES: usize = 80_000;
const DEFAULT_ITERS: usize = 10_000;
const DEFAULT_TRIALS: usize = 1;

macro_rules! print_flush {
    ($($e:tt)*) => { { print!($($e)*); std::io::stdout().flush().unwrap() } }
}

fn timed_secs<X, F: FnOnce() -> X>(f: F) -> (f32, X) {
    let now = Instant::now();
    let result = f();
    return (now.elapsed().as_secs_f32(), result);
}

fn parse_number(s: String) -> Result<usize, <usize as FromStr>::Err> {
    let (factor, s) = if let Some(t) = s.strip_suffix("k") {
        (1_000, t)
    } else if let Some(t) = s.strip_suffix("M") {
        (1_000_000, t)
    } else if let Some(t) = s.strip_suffix("m") {
        (1_000_000, t)
    } else {
        (1, &s[..])
    };
    s.parse::<usize>().map(|n| factor * n)
}

fn load_edges() -> Vec<(u32, u32)> {
    use std::fs::File;
    use std::path::Path;
    let path = Path::new("data/soc-LiveJournal1.txt");
    println!("Reading from {:?}", path);
    let file = File::open(&path).expect("couldn't open soc-LiveJournal1.txt");
    let max_edges: Option<usize> = match var("EDGES") {
        Err(VarError::NotPresent) => Some(DEFAULT_MAX_EDGES), // default
        Err(VarError::NotUnicode(_)) => panic!("EDGES not valid unicode"),
        // explicit ways to set "no limit"
        Ok(s) if s == "all" => None,
        Ok(s) => Some(parse_number(s).expect("malformed MAX_EDGES")),
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
        let line = readline.expect("read error");
        if line.is_empty() { continue }
        if line.starts_with('#') { continue }
        if max_edges.is_some_and(|n| n <= edges.len()) { break }
        let mut elts = line[..].split_whitespace();
        let v: u32 = elts.next().unwrap().parse().expect("malformed src");
        let u: u32 = elts.next().unwrap().parse().expect("malformed dst");
        edges.push((v,u));
    }
    println!("{} edges", edges.len());
    // We don't need to sort the edges or check that they are sorted since we're using
    // HashMaps for all our indexing.
    return edges;
}

#[derive(Clone,Copy)]
struct Config {
    num_trials: usize,
    num_iters: usize,
}

#[derive(Clone,PartialEq,Eq)]
struct State {
    max_vertex: u32,
    edges: Vec<(u32, u32)>,
    edge_map: HashMap<u32, Vec<u32>>,
    edge_rev: HashMap<u32, Vec<u32>>,
    paths: Vec<(u32, u32, u32)>,
    npaths: usize,
}

fn initial_query(
    edge_map: &HashMap<u32, Vec<u32>>,
    edge_rev: &HashMap<u32, Vec<u32>>,
) -> Vec<(u32, u32, u32)> {
    let mut paths: Vec<(u32, u32, u32)> = Vec::new();
    for (b, as_vec) in edge_rev.iter() {
        let Some(cs_vec) = edge_map.get(b) else { continue };
        for a in as_vec {
            for c in cs_vec {
                paths.push((*a, *b, *c))
            }
        }
    }
    return paths;
}

fn main() {
    let edges: Vec<(u32, u32)> = load_edges();
    let num_trials = match var("TRIALS") {
        Ok(s) => parse_number(s).expect("malformed TRIALS"),
        Err(VarError::NotPresent) => DEFAULT_TRIALS,
        Err(VarError::NotUnicode(_)) => panic!("TRIALS not valid unicode"),
    };
    assert!(0 < num_trials);
    let num_iters = match var("ITERS") {
        Ok(s) => parse_number(s).expect("malformed ITERS"),
        Err(VarError::NotPresent) => DEFAULT_ITERS,
        Err(VarError::NotUnicode(_)) => panic!("ITERS not valid unicode"),
    };
    let config = Config { num_trials, num_iters };
    assert!(!edges.is_empty());

    // Build indices.
    println!("\n# PHASE 0: Build indices.");
    let phase0 = Instant::now();
    // max_vertex lets us make new vertices by incrementing.
    let mut max_vertex: u32 = 0;
    let mut edge_map: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut edge_rev: HashMap<u32, Vec<u32>> = HashMap::new();
    for &(a, b) in edges.iter() {
        max_vertex = max_vertex.max(a).max(b);
        edge_map.entry(a).or_default().push(b);
        edge_rev.entry(b).or_default().push(a);
    }
    let phase0_secs = phase0.elapsed().as_secs_f32();
    println!("{phase0_secs:.2}s");
    println!("max_vertex     {max_vertex:8}");
    println!("edge_map.len() {:8}", edge_map.len());
    println!("edge_rev.len() {:8}", edge_rev.len());

    // Find all 2-length paths.
    println!("\n# PHASE 1: Initial query: find all 2-edge paths.");
    let phase1 = Instant::now();
    let paths: Vec<(u32, u32, u32)> = initial_query(&edge_map, &edge_rev);
    let phase1_secs = phase1.elapsed().as_secs_f32();
    println!("{phase1_secs:.2}s");
    println!("num paths {:13} {:5}M", paths.len(), paths.len() / 1_000_000);

    let npaths = paths.len();
    let state = State { max_vertex, edges, edge_map, edge_rev, paths, npaths };

    // Repeatedly rewrite and update matches with various strategies.

    // These strategies actually write out the new paths. Because there are many paths,
    // the cost here is dominated by memory access, which is the same across all
    // strategies, so it doesn't give a good picture of the relative cost of different
    // strategies.

    // These strategies merely count the number of paths found. This lets us more
    // accurately measure the difference between the strategies.
    let rewrite_state = phase2("REWRITES ONLY", config, &state, phase2_rewrite_only);
    // Now try computing from scratch on state2.
    if true {
        println!();
        println!("Recomputing paths from scratch after rewriting...");
        let mut times_secs: Vec<f32> = Vec::new();
        for _trial in 1..=config.num_trials {
            let recompute = Instant::now();
            let paths = initial_query(&rewrite_state.edge_map, &rewrite_state.edge_rev);
            // uh oh! this isn't matching up!
            println!("paths.len() {:11} {:5}M", paths.len(), paths.len() / 1_000_000);
            times_secs.push(recompute.elapsed().as_secs_f32());
        }
        times_secs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        print!("times             ");
        for time in times_secs.iter() { print!(" {time:.2}s"); }
        println!();
    }

    phase2("COUNT KRIS", config, &state, phase2_count_kris);
    phase2("COUNT TINY BATCH", config, &state, phase2_count_tiny_batch_delta);
    phase2("COUNT TUPLE DELTA", config, &state, phase2_count_tuple_delta);

    let mut kris_state = phase2("KRIS", config, &state, phase2_kris);
    let mut batch_state = phase2("TINY BATCH DELTA", config, &state, phase2_tiny_batch_delta);
    let mut tuple_state = phase2("TUPLE DELTA", config, &state, phase2_tuple_delta);
    // let mut kris2_state = phase2("KRIS MODIFIED", config, &state, phase2_kris_modified);

    if false {            // same results/no duplicates bug checks
        // Test that all outputs agree and there are no duplicate paths.
        // This is expensive, but a good check for bugs.
        println!();
        println!("Sorting...");
        kris_state.paths.sort(); println!("kris sorted");
        // kris2_state.paths.sort(); println!("kris2 sorted");
        tuple_state.paths.sort(); println!("tuple sorted");
        batch_state.paths.sort(); println!("batch sorted");
        println!("Checking equality...");
        // assert!(kris_state.paths == kris2_state.paths);
        assert!(tuple_state.paths == batch_state.paths);
        assert!(kris_state.paths == tuple_state.paths);
        println!("All results equal.");

        // Check for duplicates.
        println!("Checking for duplicates...");
        let n_pre = kris_state.paths.len();
        kris_state.paths.dedup();
        let n_post = kris_state.paths.len();
        if n_pre != n_post {
            println!("{} DUPLICATES FOUND!", n_pre - n_post);
        }

        // // Debug the difference in paths if there is one.
        // if kris_state.paths != batch_state.paths {
        //     let mut kris_only = 0;
        //     let mut delta_only = 0;
        //     for path in &kris_state.paths {
        //         if !batch_state.paths.contains(path) {
        //             kris_only += 1;
        //             println!(" KRIS ONLY: {path:?}");
        //         }
        //     }
        //     for path in &batch_state.paths {
        //         if !kris_state.paths.contains(path) {
        //             delta_only += 1;
        //             println!("DELTA ONLY: {path:?}");
        //         }
        //     }
        //     println!(" kris only: {kris_only:4}");
        //     println!("delta only: {delta_only:4}");
        // }
    }
}

fn phase2<F: Fn(&mut State, u32, u32, u32)>(
    name: &str,
    config: Config,
    init_state: &State,
    rewrite: F,
) -> State {
    println!("\n# PHASE 2, {name}: Repeatedly complete triangles and find new 2-edge paths.");

    let mut times_secs: Vec<f32> = Vec::new();
    print_flush!("cloned state ");
    let (clone_secs, mut state) = timed_secs(|| init_state.clone());
    println!("in {:.2}s", clone_secs);

    for trial in 1..=config.num_trials {
        if trial != 1 {
            state = init_state.clone();
        }

        let phase2 = Instant::now();
        print_flush!("trial {trial} iters  ");
        for iter in 1..=config.num_iters {
            if iter % 1_000_000 == 0 {
                print_flush!(" {}M", iter / 1_000_000);
            }
            // "Fibonacci hashing", kinda. Except we're still using modulo.
            // https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
            let edge_idx = iter.wrapping_mul(11400714819323198485) % state.edges.len();
            let (a, c) = state.edges[edge_idx];
            // Create a new vertex b.
            state.max_vertex += 1;
            let b = state.max_vertex;
            // Do rewrite & update query results.
            rewrite(&mut state, a, b, c);
        }
        println!();
        times_secs.push(phase2.elapsed().as_secs_f32());
    }

    println!("max_vertex     {:8}", state.max_vertex);
    println!("edges.len()    {:8} {:5}M", state.edges.len(), state.edges.len() / 1_000_000);
    println!("edge_map.len() {:8}", state.edge_map.len());
    println!("edge_rev.len() {:8}", state.edge_rev.len());
    println!("paths.len() {:11} {:5}M", state.paths.len(), state.paths.len() / 1_000_000);
    println!("npaths    {:13} {:5}M", state.npaths, state.npaths / 1_000_000);
    if times_secs.len() == 1 {
        println!("time            {:7.2}s", times_secs[0]);
    } else {
        times_secs.sort_by(|a,b| a.partial_cmp(b).unwrap()); // no NaNs, I hope!
        print!("times             ");
        for time in times_secs.iter() { print!(" {time:.2}s"); }
        println!();
        // println!("min time        {:7.2}s", times_secs.iter().min_by(|x, y| x.partial_cmp(y).unwrap()).unwrap());
    }
    return state
}

// ---------- REWRITE ONLY STRATEGY ----------
//
// This does not maintain query results and only performs rewrites. This provides a
// baseline/control: how much time is spent merely on rewriting plus other overheads, and
// not on maintenance?
#[allow(dead_code)]
#[inline(always)]
fn phase2_rewrite_only(state: &mut State, a: u32, b: u32, c: u32) {
    state.edges.push((a, b));
    state.edges.push((b, c));
    state.edge_map.entry(a).or_default().push(b);
    state.edge_map.entry(b).or_default().push(c);
    state.edge_rev.entry(b).or_default().push(a);
    state.edge_rev.entry(c).or_default().push(b);
}


// ---------- KRIS' REWRITING/MAINTENANCE STRATEGY ----------
#[allow(dead_code)]
#[inline(always)]
fn phase2_kris(state: &mut State, a: u32, b: u32, c: u32) {
    if a != c {
        // 1. edge(X, a) → new path edge(X,a) edge(b,c)
        if let Some(xs) = state.edge_rev.get(&a) {
            for &x in xs { state.paths.push((x, a, b)) }
        }
        // 2. edge(c, Z) → new path edge(b,c) edge(c,Z)
        if let Some(zs) = state.edge_map.get(&c) {
            for &z in zs { state.paths.push((b, c, z)) }
        }
        // 3.              new path edge(a,b) edge(b,c)
        state.paths.push((a, b, c));
    } else { // a == c
        // 1. edge(X,a) → new path edge(X,a) edge(b,c)
        if let Some(xs) = state.edge_rev.get(&a) {
            for &x in xs { state.paths.push((x, a, b)) }
        }
        // 2. edge(c,Z) → new path edge(b,c) edge(c,Z)
        if let Some(zs) = state.edge_map.get(&c) {
            for &z in zs { state.paths.push((b, c, z)) }
        }
        // 3.              new path edge(a,b) edge(b,c)
        state.paths.push((a, b, c));
        // 4.              new path edge(b,c) edge(a,b)  since c=a
        state.paths.push((b, a, b))
    }
    // Add edges (a,b) and (b,c). Update indices.
    state.edges.push((a, b));
    state.edges.push((b, c));
    state.edge_map.entry(a).or_default().push(b);
    state.edge_map.entry(b).or_default().push(c);
    state.edge_rev.entry(b).or_default().push(a);
    state.edge_rev.entry(c).or_default().push(b);
}

#[allow(dead_code)]
#[inline(always)]
fn phase2_count_kris(state: &mut State, a: u32, b: u32, c: u32) {
    if a != c {
        // 1. edge(X, a) → new path edge(X,a) edge(b,c)
        if let Some(xs) = state.edge_rev.get(&a) {
            for &_ in xs { state.npaths += 1; }
        }
        // 2. edge(c, Z) → new path edge(b,c) edge(c,Z)
        if let Some(zs) = state.edge_map.get(&c) {
            for &_ in zs { state.npaths += 1; }
        }
        // 3.              new path edge(a,b) edge(b,c)
        state.npaths += 1;
    } else { // a == c
        // 1. edge(X,a) → new path edge(X,a) edge(b,c)
        if let Some(xs) = state.edge_rev.get(&a) {
            for &_ in xs { state.npaths += 1 }
        }
        // 2. edge(c,Z) → new path edge(b,c) edge(c,Z)
        if let Some(zs) = state.edge_map.get(&c) {
            for &_ in zs { state.npaths += 1 }
        }
        // 3.              new path edge(a,b) edge(b,c)
        state.npaths += 1;
        // 4.              new path edge(b,c) edge(a,b)  since c=a
        state.npaths += 1
    }
    state.edges.push((a, b));
    state.edges.push((b, c));
    state.edge_map.entry(a).or_default().push(b);
    state.edge_map.entry(b).or_default().push(c);
    state.edge_rev.entry(b).or_default().push(a);
    state.edge_rev.entry(c).or_default().push(b);
}

#[allow(dead_code)]
#[inline(always)]
fn phase2_kris_modified(state: &mut State, a: u32, b: u32, c: u32) {
    // 1. edge(X, a) → new path edge(X,a) edge(a,b)
    if let Some(xs) = state.edge_rev.get(&a) {
        for &x in xs { state.paths.push((x, a, b)) }
    }
    // 2. edge(c, Z) → new path edge(b,c) edge(c,Z)
    if let Some(zs) = state.edge_map.get(&c) {
        for &z in zs { state.paths.push((b, c, z)) }
    }
    // 3.              new path edge(a,b) edge(b,c)
    state.paths.push((a, b, c));
    // 4. a == c     → new path edge(b,c) edge(a,b)  since c=a
    if a == c { state.paths.push((b, a, b)) }
    // Add edges (a,b) and (b,c). Update indices.
    state.edges.push((a, b));
    state.edges.push((b, c));
    state.edge_map.entry(a).or_default().push(b);
    state.edge_map.entry(b).or_default().push(c);
    state.edge_rev.entry(b).or_default().push(a);
    state.edge_rev.entry(c).or_default().push(b);
}


// ---------- TINY BATCH DELTA RULE MAINTENANCE STRATEGY ----------
#[allow(dead_code)]
#[inline(always)]
fn phase2_tiny_batch_delta(state: &mut State, a: u32, b: u32, c: u32) {
    let new_edges = [(a,b), (b,c)];

    // Run the delta rule treating new_edges as a tiny relation. This is "batch at a
    // time" but the batches are tiny - one batch per rewrite rule firing. I've
    // optimized the plan to use the fact that new_edges is small by not bothering to
    // index new_edges and always iterating over new_edges first.
    for &(x, y) in &new_edges {
        // Δedge(x,y)  edge(y,z)
        if let Some(zs) = state.edge_map.get(&y) {
            for &z in zs { state.paths.push((x, y, z)); }
        }
        // Δedge(x,y) Δedge(y,z)
        for &(y2, z) in &new_edges {
            if y == y2 { state.paths.push((x, y, z)) }
        }
    }

    //  edge(x,y) Δedge(y,z)
    for (y, z) in new_edges {
        if let Some(xs) = state.edge_rev.get(&y) {
            for &x in xs { state.paths.push((x, y, z)) }
        }
    }

    // Add edges (a,b) and (b,c). Update indices.
    // NB. edges is NOT SORTED! or if it was before, it isn't now.
    state.edges.push((a, b));
    state.edges.push((b, c));
    state.edge_map.entry(a).or_default().push(b);
    state.edge_map.entry(b).or_default().push(c);
    state.edge_rev.entry(b).or_default().push(a);
    state.edge_rev.entry(c).or_default().push(b);
}

#[allow(dead_code)]
#[inline(always)]
fn phase2_count_tiny_batch_delta(state: &mut State, a: u32, b: u32, c: u32) {
    let new_edges = [(a,b), (b,c)];

    // Run the delta rule treating new_edges as a tiny relation. This is "batch at a
    // time" but the batches are tiny - one batch per rewrite rule firing. I've
    // optimized the plan to use the fact that new_edges is small by not bothering to
    // index new_edges and always iterating over new_edges first.
    for &(_x, y) in &new_edges {
        // Δedge(x,y)  edge(y,z)
        if let Some(zs) = state.edge_map.get(&y) {
            for &_ in zs { state.npaths += 1 }
        }
        // Δedge(x,y) Δedge(y,z)
        for &(y2, _z) in &new_edges {
            if y == y2 { state.npaths += 1 }
        }
    }

    //  edge(x,y) Δedge(y,z)
    for (y, _z) in new_edges {
        if let Some(xs) = state.edge_rev.get(&y) {
            for &_ in xs { state.npaths += 1 }
        }
    }

    // Add edges (a,b) and (b,c). Update indices.
    // NB. edges is NOT SORTED! or if it was before, it isn't now.
    state.edges.push((a, b));
    state.edges.push((b, c));
    state.edge_map.entry(a).or_default().push(b);
    state.edge_map.entry(b).or_default().push(c);
    state.edge_rev.entry(b).or_default().push(a);
    state.edge_rev.entry(c).or_default().push(b);
}


// ---------- TUPLE AT A TIME DELTA RULE STRATEGY ----------
#[allow(dead_code)]
#[inline(always)]
fn phase2_tuple_delta(state: &mut State, a: u32, b: u32, c: u32) {
    let new_edges = [(a,b), (b,c)];
    // For each new edge, run single-tuple delta rules, add it to edge list,
    // and update indices.
    for (t, u) in new_edges {
        // Update query results.
        //   Δ(edge(x,y) edge(y,z))
        // = Δedge(x,y)  edge(y,z)
        if let Some(vs) = state.edge_map.get(&u) {
            for &v in vs { state.paths.push((t, u, v)) }
        }
        // + edge(x,y)  Δedge(y,z)
        if let Some(ss) = state.edge_rev.get(&t) {
            for &s in ss { state.paths.push((s, t, u)) }
        }
        // + Δedge(x,y) Δedge(y,z)
        if t == u {             // NB. this case seems never to happen.
            state.paths.push((t, t, t));
        }
        // Add edge and update indices.
        state.edges.push((t, u));
        state.edge_map.entry(t).or_default().push(u);
        state.edge_rev.entry(u).or_default().push(t);
    }
}

#[allow(dead_code)]
#[inline(always)]
fn phase2_count_tuple_delta(state: &mut State, a: u32, b: u32, c: u32) {
    let new_edges = [(a,b), (b,c)];
    // For each new edge, run single-tuple delta rules, add it to edge list,
    // and update indices.
    for (t, u) in new_edges {
        // Update query results.
        //   Δ(edge(x,y) edge(y,z))
        // = Δedge(x,y)  edge(y,z)
        if let Some(vs) = state.edge_map.get(&u) {
            for &_ in vs { state.npaths += 1 }
        }
        // + edge(x,y)  Δedge(y,z)
        if let Some(ss) = state.edge_rev.get(&t) {
            for &_ in ss { state.npaths += 1 }
        }
        // + Δedge(x,y) Δedge(y,z)
        if t == u {             // NB. this case seems never to happen.
            state.npaths += 1;
        }
        // Add edge and update indices.
        state.edges.push((t, u));
        state.edge_map.entry(t).or_default().push(u);
        state.edge_rev.entry(u).or_default().push(t);
    }
}
