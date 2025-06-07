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
// const MAX_EDGES: usize = 10_000_000;
const MAX_EDGES: usize = 100_000_000;

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

    // FINDING DIRECTED ACYCLIC TRIANGLES
    //
    // Finds triples (a,b,c) satisfying:
    //
    //     edge(a,b)  edge(b,c)  edge(a,c)
    //
    // Since the edge relation is not symmetric, this omits some connected
    // triangles, namely the "cyclic" triangles, which instead satisfy:
    //
    //     edge(a,b)  edge(b,c)  edge(c,a)
    //
    // Moreover, it can count the same three connected nodes {x,y,z} as up to 6
    // distinct triangles, namely:
    //
    //     edge(x,y)  edge(y,z)  edge(x,z)
    //     edge(x,z)  edge(z,y)  edge(x,y)
    //     edge(y,x)  edge(x,z)  edge(y,z)
    //     edge(y,z)  edge(z,x)  edge(y,x)
    //     edge(z,x)  edge(x,y)  edge(z,y)
    //     edge(z,y)  edge(y,x)  edge(z,x)
    //
    // Finally, since there are self-edges, the nodes (a,b,c) we find may not be
    // distinct.
    if false {
        let rab = ranges(edges, |t| t.0).map(|ts| tuples(ts, |t| t.1));
        let sbc = rab.clone();
        let tac = rab.clone();
        let mut found: usize = 0;
        for (a, (rb, tc)) in rab.join(tac).iter() {
            for (b, (r, sc)) in rb.join(sbc.clone()).iter() {
                for (c, (s, t)) in sc.join(tc.clone()).iter() {
                    found += 1;
                    if found % 1_000_000 == 0 {
                        let millions = found / 1_000_000;
                        println!("found {millions} million triangles");
                    }
                }
            }
        }

        println!("FOUND THEM ALL");
        dbg!(found);
    }

    // FINDING UNDIRECTED TRIANGLES
    //
    // Here we use the perhaps more familiar definition of a triangle as three
    // distinct nodes connected by edges - no matter the direction of those
    // edges. Moreover, we attempt to avoid counting any triangle more than once.
    //
    // In particular, we (first) symmetrize the edge relation and (second) avoid
    // over-counting edges by considering only those from lower- to strictly
    // higher-numbered nodes. This also excludes self-edges, enforcing
    // distinctness.
    if true {
        // estimate capacity by guessing that b < a will rule out ~1/2 the edges
        println!("Creating reverse index...");
        let mut rev_edges: Vec<(u32, u32)> = Vec::with_capacity(edges.len() / 2);
        for &(a, b) in edges {
            if b < a {
                rev_edges.push((b,a));
            }
        }
        let n_rev_edges = rev_edges.len();
        println!("Sorting reverse index of {n_rev_edges} edges...");
        rev_edges.sort();
        println!("Sorted.");

        let rev = ranges(&rev_edges, |t| t.0).map(|ts| tuples(ts, |t| t.1));
        let fwd = ranges(edges, |t| t.0).map(|ts| {
            let a = ts[0].0;
            let mut bs = tuples(ts, |t| t.1);
            // We pre-seek the inner iterator to the `a` value so that we only
            // get tuples with a < b. We could do this more declaratively, but
            // whatever.
            bs.seek(Bound::Greater(a));
            bs
        });

        // Undirected edge iterator that only allows edges from lower → higher.
        let rab = fwd.outer_join(rev);

        // Now, acyclic triangle query over this undirected edge trie.
        let sbc = rab.clone();
        let tac = rab.clone();
        let mut found: usize = 0;
        for (a, (rb, tc)) in rab.join(tac).iter() {
            for (b, (r, sc)) in rb.join(sbc.clone()).iter() {
                for (c, (s, t)) in sc.join(tc.clone()).iter() {
                    found += 1;
                    if found % 1_000_000 == 0 {
                        let millions = found / 1_000_000;
                        println!("found {millions} million triangles");
                    }
                }
            }
        }

        println!("FOUND THEM ALL");
        dbg!(found);
    }
}
