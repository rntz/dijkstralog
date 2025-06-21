#![allow(unused_imports, unused_variables)]

// Use the SNAP soc-LiveJournal1.txt data set:
// https://snap.stanford.edu/data/soc-LiveJournal1.html
// https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz

use dijkstralog::iter;
use dijkstralog::iter::{Seek, ranges, tuples, Bound};
use dijkstralog::search::Search;

use std::io::prelude::*;

// total edges in soc-LiveJournal1.txt: 68,993,773
// 20M edges takes ~7-8s on my Macbook M1 Pro
const DEFAULT_MAX_EDGES: usize = 20_000_000;

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

fn main() {
    let edges: Vec<(u32, u32)> = load_edges();
    let edges: &[(u32, u32)] = &edges;

    // Hand-optimized version. Still uses `ranges`, though; I could do with optimizing
    // that out.
    println!("Hand-coded undirected triangles.");

    let mut rev_edges: Vec<(u32, u32)> = edges
        .iter()
        .filter_map(|&(a, b)| if b < a { Some((b, a)) } else { None })
        .collect();
    rev_edges.sort();

    let rev = ranges(&rev_edges, |t| t.0).map(|bs| tuples(bs, |t| t.1));
    let fwd = ranges(edges, |t| t.0).map(|ts| {
        let a = ts[0].0;
        let mut bs = tuples(ts, |t| t.1);
        // We pre-seek the inner iterator to the `a` value so that we only
        // get tuples with a < b. We could do this more declaratively, but
        // whatever.
        bs.seek(Bound::Greater(a));
        bs
    });

    let unique_edges: Vec<(u32, u32)> = fwd
        .outer_join(rev)
        .iter()
        .flat_map(|(a, bs)| bs.keys().map(move |b| (a,b)))
        .collect();
    println!("Created unique_edges index.");

    // prevent me from messing up and accessing edges instead of unique_edges.
    let edges = ();

    // The triangle query.
    let mut found: usize = 0;
    // edge(a,b) and edge(a,c) and edge(b,c)
    for (a, bs) in ranges(&unique_edges, |t| t.0).iter() {
        let mut bs_idx = 0;     // finding edge(a,b) tuples for the given `a'
        let mut edge_idx = 0;   // finding edge(b,c) tuples
        'outer: while bs_idx < bs.len() {
            let (_, b) = bs[bs_idx];
            let edge = &unique_edges[edge_idx];
            edge_idx += unique_edges[edge_idx..].search(|&(b2, _c)| b2 < b);
            if edge_idx >= unique_edges.len() { break; }

            // If there are no edges out of b, search forward for it.
            let (b2, c) = unique_edges[edge_idx];
            if b != b2 {
                bs_idx += bs[bs_idx..].search(|&(_, b)| b < b2);
                continue;
            }

            // We've found edge(a,b). Now, find cs such that bs(a,c) and edge(b,c).
            let mut cs_idx = bs_idx + 1;
            while cs_idx < bs.len() {
                let (_, c) = bs[cs_idx];
                edge_idx += unique_edges[edge_idx..]
                    .search(|&(b3, c2)| b == b3 && c2 < c);
                if edge_idx >= unique_edges.len() { break 'outer; }

                let (b3, c2) = unique_edges[edge_idx];
                if b != b3 { break; }
                if c != c2 {
                    // advance cs_idx!
                    cs_idx += bs[cs_idx..].search(|&(_, c)| c < c2);
                    continue;
                }

                found += 1;
                if found % 1_000_000 == 0 {
                    let millions = found / 1_000_000;
                    println!("triangle {millions} million is {a} {b} {c}");
                }
                cs_idx += 1;
            }
            bs_idx += 1;
        }
    }

    println!("FOUND THEM ALL");
    dbg!(found);
}
