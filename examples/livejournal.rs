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
//const MAX_EDGES: usize = 10_000_000;
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
        println!("Directed triangle query, edge(A,B) edge(B,C) edge(A,C).");
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
        println!("Undirected triangle query, Datalog-ish approach.");

        // estimate capacity by guessing that b < a will rule out ~1/2 the edges
        println!("Creating reverse index...");
        let mut rev_edges: Vec<(u32, u32)> = edges
            .iter()
            .filter_map(|&(a, b)| if b < a { Some((b, a)) } else { None })
            .collect();
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

        // Materialize a symmetrized edge index that has all edges lower → higher. We
        // could leave this unmaterialized and just use `fwd.outer_join(rev)`, but this
        // roughly doubles total execution time for me. So we're using space to save time.
        println!("Creating symmetric edge index...");
        let unique_edges: Vec<(u32, u32)> = fwd
            .outer_join(rev)
            .iter()
            .flat_map(|(a, bs)| bs.keys().map(move |b| (a,b)))
            .collect();
        println!("done.");

        // Now, acyclic triangle query over this undirected edge trie.
        let r__ = ranges(&unique_edges, |t| t.0).map(|ts| tuples(ts, |t| t.1));
        let mut found: usize = 0;
        for (a, ra_) in r__.clone().iter() { // optimize: omit the self-join.
            for (b, (rab, rb_)) in ra_.clone().join(r__.clone()).iter() {
                for (c, (rac, rbc)) in ra_.clone().join(rb_).iter() {
                    found += 1;
                    if found % 1_000_000 == 0 {
                        let millions = found / 1_000_000;
                        println!("found {millions} million triangles");
                    }
                }
            }
        }

        // // Previous, more naive version.
        // let rab = ranges(&unique_edges, |t| t.0).map(|ts| tuples(ts, |t| t.1));
        // let sbc = rab.clone();
        // let tac = rab.clone();
        // let mut found: usize = 0;
        // for (a, (rb, tc)) in rab.join(tac).iter() {
        //     for (b, (r, sc)) in rb.join(sbc.clone()).iter() {
        //         for (c, (s, t)) in sc.join(tc.clone()).iter() {
        //             found += 1;
        //             if found % 1_000_000 == 0 {
        //                 let millions = found / 1_000_000;
        //                 println!("found {millions} million triangles");
        //             }
        //         }
        //     }
        // }

        println!("FOUND THEM ALL");
        dbg!(found);
    }

    // A simpler (in Rust) version of the above: we jump directly to the ordered
    // symmetrized edge index by directing all edges low → high in one scan over
    // the edge list, then sorting.
    //
    // However, this takes _slightly_ longer than the above, I think because it's doing
    // more sorting work. In the above, we only sort `rev_edges`, while the outer join
    // between fwd and rev is a fast sorted-list merge. Here we sort the entirety of
    // `unique_edges`, roughly twice as much data; we're failing to take advantage of
    // `edges` being pre-sorted.
    if false {
        println!("Simpler approach to undirected triangles.");

        // QUESTION: This pre-indexing step is natural and simple, but how do I
        // express it to a Datalog engine built on worst-case optimal joins? It
        // does not proceed in variable-at-a-time fashion, and the only reason
        // it doesn't produce duplicates is the strictness of the order
        // comparison.
        let mut unique_edges: Vec<(u32, u32)> = Vec::with_capacity(edges.len());
        for &(a, b) in edges {
            if a == b { continue }
            unique_edges.push(if a < b { (a, b) } else { (b, a) })
        }
        let n_edges = unique_edges.len();
        println!("Materialized list of {n_edges} symmetric non-self edges, sorting...");
        unique_edges.sort();
        println!("Sorted.");

        // Now we just do a regular triangle query over unique_edges.
        let iter = ranges(&unique_edges, |t| t.0).map(|ts| tuples(ts, |t| t.1));
        let mut found: usize = 0;
        for (a, (rb, tc)) in iter.clone().join(iter.clone()).iter() {
            for (b, (r, sc)) in rb.join(iter.clone()).iter() {
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

    // Hand-optimized version. Still uses `ranges`, though; I could do with optimizing
    // that out.
    if false {
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
            let mut bs_idx = 0;
            let mut edge_idx = 0;
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

                // We've found edge(a,b). Now, find cs such that bs(a,c) and edge(b,c)
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
}
