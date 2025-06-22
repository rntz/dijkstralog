#![allow(unused_variables)]

// Use the SNAP soc-LiveJournal1.txt data set:
// https://snap.stanford.edu/data/soc-LiveJournal1.html
// https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz

use std::io::prelude::*;

use std::collections::{HashMap, HashSet};

// Takes ≤ 3s on my Macbook M1 Pro.
// Set EDGES environment variable to override; EDGES=all for no limit.
const DEFAULT_MAX_EDGES: usize = 80_000;

fn load_edges() -> Vec<(u32, u32)> {
    // TODO: use first std::env::args as data file if present
    use std::fs::File;
    use std::path::Path;
    let path = Path::new("data/soc-LiveJournal1.txt");
    let file = File::open(&path).expect("couldn't open soc-LiveJournal1.txt");
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
        let line = readline.expect("read error");
        if line.is_empty() { continue }
        if line.starts_with('#') { continue }
        if max_edges.is_some_and(|n| n <= edges.len()) { break }
        let mut elts = line[..].split_whitespace();
        let v: u32 = elts.next().unwrap().parse().expect("malformed src");
        let u: u32 = elts.next().unwrap().parse().expect("malformed dst");
        edges.push((v,u));
    }
    print!("{} edges", edges.len());
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
    let edges: Vec<(u32, u32)> = load_edges();
    if DEBUG {
        println!("edges: {:?}", edges);
    }

    println!("Generating edge hash index...");
    let mut edge_map: HashMap<u32, Vec<u32>> = HashMap::new();
    for &(a, b) in edges.iter() {
        edge_map.entry(a).or_default().push(b);
    }
    let edge_map = edge_map;
    println!("done.");

    println!("Generating reverse edge hash index...");
    let mut rev_edge_map: HashMap<u32, Vec<u32>> = HashMap::new();
    for &(a, b) in edges.iter() {
        rev_edge_map.entry(b).or_default().push(a);
    }
    let rev_edge_map = rev_edge_map;
    println!("done.");

    // trans a b <- edge a b
    // trans a c <- trans a b, edge b c
    //
    // implemented as
    //
    // Δtrans 0     a b = edge a b
    // Δtrans (i+1) a c = Δtrans i a b * edge b c
    //  trans (i+1)     = trans i + Δtrans i
    //
    //
    // Δtrans_rev' c a = Δtrans_rev b a * edge b c

    let mut n = 0usize;
    let mut trans: HashSet<(u32, u32)> = HashSet::new();
    let mut worklist: Vec<(u32, u32)> = edges;

    println!("Finding paths...");

    while let Some((a, b)) = worklist.pop() {
        if !trans.insert((a,b)) {
            // insert returns false if trans already contained (a,b); no further
            // work to be done.
            // println!(" {a} -- {b} dup");
            continue
        } else {
            // println!(" {a} -- {b}");
        }
        n += 1;
        if n % 1_000_000 == 0 {
            let millions = n / 1_000_000;
            println!("  found {millions} million paths");
        }

        if true {
            let Some(cs) = edge_map.get(&b) else { continue };
            for c in cs {
                // println!("   {a} -- {c} enqueue");
                worklist.push((a, *c));
            }
        } else { // computes the same thing but noticeably slower.
            let Some(pres) = rev_edge_map.get(&a) else { continue };
            for pre in pres {
                worklist.push((*pre, b));
            }
        }
    }

    println!("Found {n} ≈ {n:.0e} paths");
}
