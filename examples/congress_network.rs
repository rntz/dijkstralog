#![allow(unused_imports, unused_variables)]

use dijkstralog::iter;
use dijkstralog::iter::{Seek, ranges, tuples};

use std::fs::File;
use std::io::prelude::*;
use std::io::{BufRead, BufReader};
use std::path::Path;

const MAX_EDGES: usize = 15000;
// const MAX_EDGES: usize = 1000;

fn main() {
    let path = Path::new("data/congress_network/congress.edgelist");
    let file = BufReader::new(File::open(&path).expect("couldn't open congress.edgelist"));

    let mut edges: Vec<(u32, u32)> = Vec::new();

    for (n, readline) in file.lines().enumerate() {
        let line = readline.expect("read error");
        if line.is_empty() { continue; }

        if MAX_EDGES <= n { break }

        let (v, rest) = line.split_once(" ").expect("not enough values on line {n}");
        let (u, ____) = rest.split_once(" ").expect("not enough values on line {n}");
        // TODO: grab the weight as well.

        let v = v.parse().expect("Expected integer, got {v}");
        let u = u.parse().expect("Expected integer, got {u}");
        edges.push((v,u));
    }

    let n_edges = edges.len();
    if edges.is_sorted() {
        println!("{n_edges} edges (already sorted)");
    } else {
        println!("sorting {n_edges} edges...");
        edges.sort();
    }
    dbg!(edges.len());

    let edges: &[(u32, u32)] = &edges;
    let iter = ranges(edges, |t| t.0).map(|ts| ranges(ts, |t| t.1).map(|ts| ts.len()));

    let rab = iter.clone();
    let sbc = iter.clone();
    let tac = iter.clone();

    let mut triangles = Vec::new();
    for (a, (rb, tc)) in rab.join(tac).iter() {
        for (b, (r, sc)) in rb.join(sbc.clone()).iter() {
            for (c, (s, t)) in sc.join(tc.clone()).iter() {
                assert!(r * s * t == 1);
                triangles.push((a, b, c))
            }
        }
    }

    let n_triangles = triangles.len();
    dbg!(triangles.len());

    // Let's compute the # of triangles directly.
    let mut real_triangles: Vec<(u32, u32, u32)> = Vec::new();
    for &(a, b) in edges {
        for &(b2, c) in edges {
            if b != b2 { continue }
            for &(a2, c2) in edges {
                if a != a2 || c != c2 { continue }
                real_triangles.push((a, b, c))
            }
        }
    }
    dbg!(real_triangles.len());

    dbg!(triangles == real_triangles);
    assert!(triangles.is_sorted() && real_triangles.is_sorted());
    assert!(triangles == real_triangles);
}
