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

use std::io::prelude::*;
use std::time::Instant;

use dijkstralog::iter;
use dijkstralog::iter::{Seek, ranges, tuples, Bound};

// total edges in soc-LiveJournal1.txt: 68,993,773
// 20M edges takes ~8-9s on my Macbook M1 Pro
const DEFAULT_MAX_EDGES: usize = 20_000_000;
const DEFAULT_FILE: &str = "data/soc-LiveJournal1.txt";

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

    let beginning = Instant::now();

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
    //
    // As a Datalog program:
    //
    // edge_directed(a,b).
    //
    // edge_symmetric(a,b) :- edge_directed(a,b), a < b.
    // edge_symmetric(a,b) :- edge_directed(b,a), a < b.
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

        // a, b, c
        // R(a,b) R(b,c) R(a,c)
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

    println!("Finding triangles (excluding loading, including index construction) took {:.2}s.", beginning.elapsed().as_secs_f32());
}
