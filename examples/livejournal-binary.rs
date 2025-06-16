#![allow(unused_imports, unused_variables)]

// Use the SNAP soc-LiveJournal1.txt data set:
// https://snap.stanford.edu/data/soc-LiveJournal1.html
// https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz

use dijkstralog::nest;
use dijkstralog::iter;
use dijkstralog::iter::{Seek, ranges, tuples, Bound};
use dijkstralog::search::Search;

use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;

// total edges in soc-LiveJournal1.txt: 68,993,773
const MAX_EDGES: usize = 20_000_000;
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

    println!("Undirected triangles, materializing intermediates.");

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
    let edges = ();             // nevermore.
    let unique_edges: &[(u32, u32)] = &unique_edges[..];

    // candidate(b,c,a) = unique_edge(a,b) and unique_edge(a,c) // NEEDS SORT!
    // result(b,c,a) = candidate(b,c,a) and unique_edge(b,c)
    // This takes WAAAY too much time sorting.
    if false {
        println!("Materializing candidates...");
        let mut candidates: Vec<(u32, u32, u32)> = Vec::new();
        dijkstralog::nest! {
            for (a, bs) in ranges(unique_edges, |x| x.0).iter();
            for (i, &(_, b)) in bs.iter().enumerate();
            for &(_, c) in bs[i+1..].iter();
            candidates.push((b, c, a))
        }
        println!("Found {} candidates, sorting...", candidates.len());
        candidates.sort();
        println!("Sorted.");        // THIS TAKES THE VAST MAJORITY OF THE TIME.
        let candidates: &[(u32, u32, u32)] = &candidates[..];

        // result(a,b,c) = candidate(a,b,c) and unique_edge(b,c)
        // for this we want to sort differently! augh!
        println!("Joining against edges...");
        let mut results: Vec<(u32, u32, u32)> = Vec::new();
        dijkstralog::nest! {
            for ((b,c), (as_, _)) in ranges(candidates, |x| (x.0, x.1))
                .join(tuples(unique_edges, |x| *x))
                .iter();
            for &(_, _, a) in as_;
            results.push((a, b, c))
        };
        println!("Found {} results", results.len());
    }

    // The final join is expensive but still faster than sorting.
    if true {
        println!("Materializing candidates...");
        let mut candidates: Vec<(u32, u32, u32)> = Vec::new();
        dijkstralog::nest! {
            for (a, bs) in ranges(unique_edges, |x| x.0).iter();
            for (i, &(_, b)) in bs.iter().enumerate();
            for &(_, c) in bs[i+1..].iter();
            candidates.push((a, b, c))
        }
        println!("Found {} candidates.", candidates.len());
        let candidates: &[(u32, u32, u32)] = &candidates[..];

        // result(a,b,c) = candidate(a,b,c) and unique_edge(b,c)
        println!("Joining against edges...");
        let mut results: Vec<(u32, u32, u32)> = Vec::new();
        let mut found = 0usize;
        dijkstralog::nest! {
            for (a, bcs) in ranges(candidates, |x| x.0).iter();
            for ((b,c), _) in tuples(bcs, |x| (x.1, x.2))
                .join(tuples(unique_edges, |x| *x))
                .iter();
            {
                found += 1;
                if found % 1_000_000 == 0 {
                    println!("found {} million triangles!", found / 1_000_000);
                }
                results.push((a, b, c))
            }
        };
        println!("Found {} results", results.len());
    }

    // This is the fastest. But does it correspond to a "real" binary join plan
    // that can be executed without WCOJ and with no additional indices?
    //
    // candidate(a,b,c) = unique_edge(a,b), unique_edge(b,c)
    // result(a,b,c) = candidate(a,b,c), unique_edge(a,c)
    if false {
        println!("Materializing candidates...");
        let mut candidates: Vec<(u32, u32, u32)> = Vec::new();
        dijkstralog::nest! {
            for (a, bs) in ranges(unique_edges, |x| x.0).iter();
            for (b, (_, cs)) in tuples(bs, |x| x.1)
                .join(ranges(unique_edges, |x| x.0))
                .iter();
            for &(_, c) in cs;
            candidates.push((a,b,c))
        }
        println!("Found {} candidates.", candidates.len());
        let candidates: &[(u32, u32, u32)] = &candidates[..];

        // result(a,b,c) = candidate(a,b,c) and unique_edge(a,c)
        println!("Joining...");
        let mut results: Vec<(u32, u32, u32)> = Vec::new();
        let mut found = 0usize;
        dijkstralog::nest! {
            for (a, (bcs, cs1)) in ranges(candidates, |x| x.0)
                .join(ranges(unique_edges, |x| x.0))
                .iter();
            for (b, cs2) in ranges(bcs, |x| x.1).iter();
            for (c, _) in tuples(cs1, |x| x.1)
                .join(tuples(cs2, |x| x.2))
                .iter();
            {
                found += 1;
                if found % 1_000_000 == 0 {
                    println!("found {} million triangles!", found / 1_000_000);
                }
                results.push((a, b, c))
            }
        }
        println!("Found {} results.", results.len());
    }

}
