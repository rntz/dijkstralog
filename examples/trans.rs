#![allow(unused_imports, unused_variables, unreachable_code, unused_mut)]

// Use the SNAP soc-LiveJournal1.txt data set:
// https://snap.stanford.edu/data/soc-LiveJournal1.html
// https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz

use dijkstralog::iter;
use dijkstralog::iter::{Seek, ranges, tuples, Bound};
use dijkstralog::lsm;
use dijkstralog::lsm::{LSM, Layer};

use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;

// total edges in soc-LiveJournal1.txt: 68,993,773
//const MAX_EDGES: usize = 48;
//const MAX_EDGES: usize = 70_000;
const MAX_EDGES: usize = 90_000;
//const MAX_EDGES: usize = 100_000_000;

fn load_livejournal() -> Vec<(u32, u32)> {
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

    return edges;
}

const DEBUG: bool = false;

fn main() {
    let edges: Vec<(u32, u32)> = load_livejournal();
    // TODO: Instead of this, I could just sort edges by the reverse lexical!
    let mut rev_edges: Vec<(u32, u32)> = edges.iter().map(|&(a,b)| (b,a)).collect();
    rev_edges.sort();
    let rev_edges = rev_edges.as_slice();
    if DEBUG {
        println!("edges: {:?}", edges.as_slice());
        println!("rev_edges: {:?}", rev_edges);
    }

    #[allow(unused_mut)]
    let mut trans: LSM<((u32, u32), ())> = LSM::new();
    #[allow(unused_mut)]
    let mut delta_trans: Layer<((u32, u32), ())> = Layer::from_sorted(
        edges.iter().map(|&kv| (kv, ())).collect()
    );


    let mut itercount = 0;

    // trans(a,b) <- edge(a,b)
    // trans(a,c) <- edge(a,b), trans(b,c)
    while !delta_trans.as_slice().is_empty() {
        println!("\n-- iter {itercount} --");
        let ntrans = trans.layers().map(|l| l.len()).sum::<usize>();
        let ndelta = delta_trans.len();
        println!("trans (overcount): {ntrans:>10} ≈ {ntrans:3.0e}");
        println!("      delta_trans: {ndelta:>10} ≈ {ndelta:3.0e}");
        itercount += 1;

        // Delta rule:
        // Δtrans'(a,c) <- rev_edge(b,a), Δtrans(b,c), ¬trans(a,c)
        let mut new_delta_trans: Vec<((u32, u32), ())> = Vec::new();
        {
            // We antijoin with trans to remove tuples we already know about.
            // I'm not sure whether it's better to do this here, or as a
            // post-processing step.
            // let mut trans_seek = trans.seek(
            //     |slice| tuples(slice, |kv| kv.0).map(|_| ())
            // );
            //
            // TODO FIXME: This actually generates duplicates! It deduplicates
            // against trans, but not against delta_trans! weird!
            dijkstralog::nest! {
                for (_b, (delta_slice, rev_edge_slice)) in
                    ranges(delta_trans.as_slice(), |x| x.0.0)
                    .join(ranges(rev_edges, |x| x.0))
                    .iter();
                // println!("trans_seek b={_b}");
                let mut trans_seek = trans.seek(
                    |slice| tuples(slice, |kv| kv.0).map(|_| ())
                );
                for &(_b, a) in rev_edge_slice;
                for &((_b, c), ()) in delta_slice;
                // wait, shit. don't we need to do multiple passes over trans_seek here??
                // argh!!!
                if trans_seek.seek_to((a,c)).is_none() {
                    // NB. at this point we can generate a duplicate (a,c) tuple!
                    if DEBUG { println!("  found {a:>2} -- {_b:2} -* {c:<2}"); }
                    new_delta_trans.push(((a, c), ()))
                } else {
                    if DEBUG { println!("        {a:>2} -- {_b:2} -* {c:<2} omitted"); }
                }
            }
        }

        // Update rules:
        // trans' <- trans + Δtrans
        trans.push(delta_trans);
        // This^ consumes delta_trans! implications for evaluation order of full
        // Datalog system?

        new_delta_trans.sort();
        new_delta_trans.dedup(); // IMPORTANT
        if DEBUG {
            println!(
                "new deltas: {:?}",
                new_delta_trans.as_slice()
                    .iter()
                    .map(|(kv, _)| kv)
                    .collect::<Vec<_>>()
            );
        }
        delta_trans = Layer::from_sorted(new_delta_trans);
    }

    println!();
    let paths: Vec<(u32, u32)> = trans.into_iter().map(|x| x.0).collect();
    println!("Found {} = {:.2e} paths", paths.len(), paths.len());
    if DEBUG {
        println!("paths: {:?}", paths.as_slice());
    }
    // TODO: debug-print the layers of our LSM.
}
