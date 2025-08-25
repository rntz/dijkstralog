#![allow(unused_imports, unused_variables)]

// Maintaining 2-edge query
// against triangle-completion rewrite rule.
//
// QUERY    {1,2,3} edge(1,2) edge(2,3)
// REWRITE  {a,c} edge(a,c) → {a,b,c} edge(a,c) edge(a,b) edge(b,c)
//
// MAINTENANCE CASES
//
// STRATEGY FOR REWRITING
//
// Repeatedly pick an edge(a,c) from the current edge-set at random. The rewrite
// isn't idempotent/monic so there's no point in splitting into "processed"
// edges and "unprocessed" edges to try to hit a fixed point.
//
// STRATEGY FOR QUERY MAINTENANCE
//
// Use hash-indices everywhere.

use std::io::prelude::*;
use std::collections::{HashMap, HashSet};
use std::env::{var, VarError};
use std::str::FromStr;
use std::time::{Instant, Duration};

use dijkstralog::iter::{Seek, ranges, tuples};
use dijkstralog::lsm::{LSM, Layer, Key};

// Takes ≤ 3s on my Macbook M1 Pro.
// Set EDGES environment variable to override; EDGES=all for no limit.
const DEFAULT_MAX_EDGES: usize = 80_000;
const DEFAULT_ITERS: usize = 10_000;

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

fn main() {
    let edges: Vec<(u32, u32)> = load_edges();
    let num_iters = match var("ITERS") {
        Ok(s) => parse_number(s).expect("malformed ITERS"),
        Err(VarError::NotPresent) => DEFAULT_ITERS,
        Err(VarError::NotUnicode(_)) => panic!("ITERS not valid unicode"),
    };
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
    let mut paths: Vec<(u32, u32, u32)> = Vec::new();
    for (b, as_vec) in edge_rev.iter() {
        let Some(cs_vec) = edge_map.get(b) else { continue };
        for a in as_vec {
            for c in cs_vec {
                paths.push((*a, *b, *c))
            }
        }
    }
    let phase1_secs = phase1.elapsed().as_secs_f32();
    println!("{phase1_secs:.2}s");
    println!("num paths {:13} {:5}M", paths.len(), paths.len() / 1_000_000);

    // Repeatedly rewrite and update matches (Kris strategy).
    let state = State { max_vertex, edges, edge_map, edge_rev, paths };
    print_flush!("cloning state ");
    let (clone_secs, state2) = timed_secs(|| state.clone());
    println!("took {:.2}s", clone_secs);
    phase2("KRIS", num_iters, state2, phase2_kris);

    // Repeatedly rewrite and update matches (Kris strategy).
    phase2("KRIS MODIFIED", num_iters, state.clone(), phase2_kris_modified);

    // Repeatedly rewrite and update matches (tuple-at-a-time delta rules).
    phase2("TINY BATCH DELTA", num_iters, state.clone(), phase2_delta_tiny_batch);

    // Repeatedly rewrite and update matches (tuple-at-a-time delta rules).
    phase2("TUPLE DELTA", num_iters, state, phase2_delta_tuple);
}

#[derive(Clone)]
struct State {
    max_vertex: u32,
    edges: Vec<(u32, u32)>,
    edge_map: HashMap<u32, Vec<u32>>,
    edge_rev: HashMap<u32, Vec<u32>>,
    paths: Vec<(u32, u32, u32)>,
}

fn phase2<F: Fn(&mut State, u32, u32, u32)>(
    name: &str,
    num_iters: usize,
    mut state: State,
    rewrite: F,
) {
    println!("\n# PHASE 2, {name}: Repeatedly complete triangles and find new 2-edge paths.");
    let phase2 = Instant::now();

    for iter in 1..=num_iters {
        if iter % 1_000_000 == 0 {
            println!("{} million iterations", iter / 1_000_000);
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

    let phase2_secs = phase2.elapsed().as_secs_f32();
    println!("{phase2_secs:.2}s");
    println!("max_vertex     {:8}", state.max_vertex);
    println!("edges.len()    {:8} {:5}M", state.edges.len(), state.edges.len() / 1_000_000);
    println!("edge_map.len() {:8}", state.edge_map.len());
    println!("edge_rev.len() {:8}", state.edge_rev.len());
    println!("num paths {:13} {:5}M", state.paths.len(), state.paths.len() / 1_000_000);
}

// Kris' rewriting/maintenance strategy.
#[inline(always)]
fn phase2_kris(state: &mut State, a: u32, b: u32, c: u32) {
    if a != c {
        // 1. edge(X, a) → new path edge(X,a) edge(b,c)
        if let Some(xs) = state.edge_rev.get(&a) {
            for &x in xs { state.paths.push((x, a, b)) }
        }
        // 2. edge(c, Z) → new path edge(b,c) edge(c,Z)
        if let Some(zs) = state.edge_map.get(&c) {
            for &z in zs { state.paths.push((a, b, z)) }
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
            for &z in zs { state.paths.push((a, b, z)) }
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

#[inline(always)]
fn phase2_kris_modified(state: &mut State, a: u32, b: u32, c: u32) {
    // 1. edge(X, a) → new path edge(X,a) edge(b,c)
    if let Some(xs) = state.edge_rev.get(&a) {
        for &x in xs { state.paths.push((x, a, b)) }
    }
    // 2. edge(c, Z) → new path edge(b,c) edge(c,Z)
    if let Some(zs) = state.edge_map.get(&c) {
        for &z in zs { state.paths.push((a, b, z)) }
    }
    // 3.              new path edge(a,b) edge(b,c)
    state.paths.push((a, b, c));
    // 4. a == c     → new path edge(b,c) edge(a,b)  since c=a
    if a ==c { state.paths.push((b, a, b)) }
    // Add edges (a,b) and (b,c). Update indices.
    state.edges.push((a, b));
    state.edges.push((b, c));
    state.edge_map.entry(a).or_default().push(b);
    state.edge_map.entry(b).or_default().push(c);
    state.edge_rev.entry(b).or_default().push(a);
    state.edge_rev.entry(c).or_default().push(b);
}

#[inline(always)]
fn phase2_delta_tiny_batch(state: &mut State, a: u32, b: u32, c: u32) {
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

#[inline(always)]
fn phase2_delta_tuple(state: &mut State, a: u32, b: u32, c: u32) {
    let new_edges = [(a,b), (b,c)];
    // For each new edge, run single-tuple delta rules, add it to edge list,
    // and update indices.
    for (v, u) in new_edges {
        // Update query results.
        //   Δ(edge(x,y) edge(y,z))
        // = Δedge(x,y)  edge(y,z)
        if let Some(zs) = state.edge_map.get(&u) {
            for &z in zs { state.paths.push((v, u, z)) }
        }
        // + edge(x,y)  Δedge(y,z)
        if let Some(xs) = state.edge_rev.get(&v) {
            for &x in xs { state.paths.push((x, v, u)) }
        }
        // + Δedge(x,y) Δedge(y,z)
        if u == v {             // NB. this case seems never to happen.
            state.paths.push((v, u, v));
        }
        // Add edge and update indices.
        state.edges.push((v, u));
        state.edge_map.entry(v).or_default().push(u);
        state.edge_rev.entry(u).or_default().push(v);
    }
}
