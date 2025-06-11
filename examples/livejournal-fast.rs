#![allow(unused_imports, unused_variables)]

// Use the SNAP soc-LiveJournal1.txt data set:
// https://snap.stanford.edu/data/soc-LiveJournal1.html
// https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz

// FUTURE WORK: Parallelisation strategy, suggested by Tyler Hou:
//
// triangles_in(A) = triangles(A, A, A)
//
// triangles(A + B, A + B, A + B)
// = triangles(A, A, A)
// + triangles(A, A, B)
// + triangles(A, B, A)
// + triangles(A, B, B)
// + triangles(B, A, A)
// + triangles(B, A, B)
// + triangles(B, B, A)
// + triangles(B, B, B)
//
// We can execute these in parallel.
// We can partition edges into A,B by hash-bucketing.

use dijkstralog::iter;
use dijkstralog::iter::{Seek, ranges, tuples, Bound, Outer};

use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;

// total edges in soc-LiveJournal1.txt: 68,993,773
const MAX_EDGES: usize = 10_000_000;
//const MAX_EDGES: usize = 100_000_000;

fn main() {
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
            edge_idx += unique_edges[edge_idx..].partition_point(|&(b2, _c)| b2 < b);
            if edge_idx >= unique_edges.len() { break; }

            // If there are no edges out of b, search forward for it.
            let (b2, c) = unique_edges[edge_idx];
            if b != b2 {
                bs_idx += bs[bs_idx..].partition_point(|&(_, b)| b < b2);
                continue;
            }

            // We've found edge(a,b). Now, find cs such that bs(a,c) and edge(b,c).
            let mut cs_idx = bs_idx + 1;
            while cs_idx < bs.len() {
                let (_, c) = bs[cs_idx];
                edge_idx += unique_edges[edge_idx..]
                    .partition_point(|&(b3, c2)| b == b3 && c2 < c);
                if edge_idx >= unique_edges.len() { break 'outer; }

                let (b3, c2) = unique_edges[edge_idx];
                if b != b3 { break; }
                if c != c2 {
                    // advance cs_idx!
                    cs_idx += bs[cs_idx..].partition_point(|&(_, c)| c < c2);
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
