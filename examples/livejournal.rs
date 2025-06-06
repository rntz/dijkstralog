#![allow(unused_imports, unused_variables)]

use dijkstralog::iter;
use dijkstralog::iter::{Seek, ranges, tuples};

use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;

// total edges in soc-LiveJournal1.txt: 68,993,773
//const MAX_EDGES: usize = 7_000_000;
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
    let iter = ranges(edges, |t| t.0).map(|ts| ranges(ts, |t| t.1).map(|ts| ts.len()));

    let rab = iter.clone();
    let sbc = iter.clone();
    let tac = iter.clone();

    let mut found: usize = 0;
    #[allow(unused_mut)] let mut triangles: Vec<(u32, u32, u32)> = Vec::new();
    for (a, (rb, tc)) in rab.join(tac).iter() {
        for (b, (r, sc)) in rb.join(sbc.clone()).iter() {
            for (c, (s, t)) in sc.join(tc.clone()).iter() {
                assert!(r * s * t == 1);
                // triangles.push((a, b, c));
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
    return;

    #[allow(unreachable_code)]
    let n_triangles = triangles.len();
    dbg!(triangles.len());

    // // Let's compute the # of triangles directly.
    // let mut real_triangles: Vec<(u32, u32, u32)> = Vec::new();
    // for &(a, b) in edges {
    //     for &(b2, c) in edges {
    //         if b != b2 { continue }
    //         for &(a2, c2) in edges {
    //             if a != a2 || c != c2 { continue }
    //             real_triangles.push((a, b, c))
    //         }
    //     }
    // }
    // dbg!(real_triangles.len());

    // dbg!(triangles == real_triangles);
    // assert!(triangles.is_sorted() && real_triangles.is_sorted());
    // assert!(triangles == real_triangles);
}
