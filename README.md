This README was written as of 20 January 2026, d4a7cbb45ea3f1b9db4316e5044a972bbf716b8a, and may since be out of date.


# What is this?

An iterator interface and implementations of it that make it easy(-ish) to express worst-case optimal joins, called “WCOI” for “worst-case optimal iterators”. Related to [indexed streams](https://dl.acm.org/doi/abs/10.1145/3591268). The best currently available description of my particular approach is my miniKanren 2025 workshop paper [Fair intersection of seekable iterators](https://arxiv.org/abs/2510.26016v1) and [its talk](https://www.youtube.com/watch?v=-32fwqirjW8).


# Directory & file guide

- `src`: The Rust implementation. See [Implementation](#implementation), below.

- `examples`: Example programs. See [Examples](#examples), below.

- `macros`: A subpackage containing some not-yet-working procedural macros for writing Datalog-style queries and rules. See [Macros](#macros), below.

- `minikanren2025`: Stuff for my miniKanren 2025 paper [Fair intersection of seekable iterators](https://arxiv.org/abs/2510.26016v1). Also contains various attempts (`minikanren2025/*.{rs,hs}`) to optimize performance of seekable iterators by tweaking the interface.

There are various `*.{hs,jl,rkt.py}` files in the root directory. These are mostly experiments with expressing worst-case optimal iterators in various languages. I started in Haskell so there are a lot of those. Probably the best one to read is `iters_alltogethernow.hs` or `iters_alltogethernow_minimal.hs`.


# Implementation

- `src/lib.rs`: The entry point.

- `src/search.rs`: Implementations of various [galloping/exponential search](https://en.wikipedia.org/wiki/Exponential_search) strategies. Galloping search finds the index i an element should take in a sorted array of n elements in time O(log i). So it has the same O(log n) worst case as binary search, but performs much better if the element is near the start of the array. This makes it useful for adaptive set intersections.

- `src/iter.rs`: The core `Seek` interface for worst-case optimal iterators, and implementations of it for simple data structures (mostly sorted vectors).

- `src/macros.rs`: The convenience macros `relationize!` and `nest!`; see [Macros](#macros), below.

- `src/lsm.rs`: Implements an LSM tree of sorted vectors and `Seek`able iterators over it. The core idea is taken from [this Frank McSherry blog post](https://github.com/frankmcsherry/blog/blob/master/posts/2025-06-03.md#data-oriented-design--columns) (search for ‘FactLSM’). The purpose is to have a sorted data structure with a reasonably efficient way to merge new sorted vectors into it. A B-tree variant would also have worked but this is simpler.

- `src/seek2.rs` and `src/seek_token.rs`: Some so-far unused experiments with alternative `Seek` interfaces.

- `src/negation.rs`: Experimental, untested antijoin/negation support.


# Examples

Many examples require [SNAP datasets](https://snap.stanford.edu/data/) to run, and expect it to be in a `data/` directory. Because the iterators are super slow without optimization and inlining, these examples generally won't run to completion in dev mode; use `cargo run --release --example NAME`. Examples that don't use any dataset and run without `--release`:

- `basic.rs`: Basic usage examples.
- `macros.rs`: Examples of some simple macros. See [Macros](#macros).
- `gallop.rs`: I've forgotten what this does.

**SNAP dataset examples:**

Many of these (but not all, sorry!) check the environment variable `EDGES` for how many edges to read from their dataset (often the whole dataset is too large), picking some default if not given. They accept simple suffixes: `k` for one thousand, `M` or `m` for one million, e.g. `EDGES=12m cargo run --release --example triangle`. Probably the most interesting examples for benchmarking purposes are `triangle.rs` for triangle counting, the `trans{,2,2_parallel}.rs` family for transitive closure, and `min_reachable` if you're interested in aggregations.

- `congress_network.rs`: Calculate number of triangles in the [Congress twitter interaction network](https://snap.stanford.edu/data/congress-twitter.html).

- `triangle.rs`: Counts triangles. Uses [LiveJournal][] by default, but will use the first command-line argument if provided.

- `triangle-fast.rs`: Handcoded triangle finding loop (worst-case optimal but _not_ using WCOIs) in the [LiveJournal][] dataset.

- `trans.rs`, `trans2.rs`, `trans2_parallel.rs`, `trans3.rs`: Transitive closure implementations. These mostly default to the [High Energy Physics Phenomenology citation network (ca-HepPh)][ca-HepPh], but you can provide an alternative file as their first command-line argument. Transitive closure is much more explosive than triangle counting, so beware of giving these too many edges! `trans.rs` is the most straightforward implementation. `trans2` implements a trick to speed up a re-sorting step necessary when projecting away the intermediate vertex when joining new paths against edges. `trans2_parallel` additionally parallelizes the inner loop using [Rayon](https://docs.rs/rayon/latest/rayon/) by partitioning the delta; this can give a big speedup on multicore machines. `trans3` uses a slightly different diff minimization strategy (not a huge effect).

- `trans_rev.rs`, `trans_rev2.rs`: Older variants of the transitive closure code with a slightly different strategy (indexing in a different order). Seem to perform worse although I don't know why. They use [LiveJournal][].

- `trans_hash.rs`: Transitive closure of [LiveJournal][] using hashtables and a simple tuple-at-a-time worklist rather than WCOIs.

- `min_reachable.rs`: A variant of transitive closure: computes, for every vertex v in a graph, the minimum-labelled vertex u reachable from it. Shows how to do aggregations/tensor contraction/semiring-weighted logic programming with WCOIs. Defaults to [ca-HepPh][] but changeable by command-line argument.

- `livejournal-binary.rs`: This computes triangles in the [LiveJournal][] dataset using a non-worst-case-optimal binary join plan approach. I haven't touched this in a while, I'm not sure what state it's in.

- `triangle_extension.rs`: A graph rewriting workload. Uses the [LiveJournal dataset][LiveJournal]. TODO: more details.

**Graspan dataset examples:** `graspan.rs` and `graspan2.rs` need a Graspan dataset. TODO: describe how to get this, if it's still possible. TODO: describe what these do. They're simple static analyses (basically reachability) based on a Frank McSherry blog post.

[LiveJournal]: https://snap.stanford.edu/data/soc-LiveJournal1.html
[ca-HepPh]: https://snap.stanford.edu/data/ca-HepPh.html


# Macros

(See `macros/src/lib.rs` and `src/macros.rs` and `examples/macros.rs`.)

Eventually I'd like to replace nested for loops with Datalog-y syntax for joins. For instance, instead of three nested for-loops for a triangle join, like in `examples/triangle.rs`:

    for (a, ra_) in r__.clone().iter() {
        for (b, (rab, rb_)) in ra_.clone().join(r__.clone()).iter() {
            for (c, (rac, rbc)) in ra_.clone().join(rb_).iter() {
                found += 1;
            }
        }
    }

which is quite awkward to read, you could instead write something like:

    rule! {
        triangles(a,b,c) <- r(a,b), r(b,c), r(a,c).
    }

And have it get automatically desugared. I haven't implemented this, but there's a procedural macro syntax parser for it in `macros/src/lib.rs`.

I also have two working macros, `relationize!` and `nest!`, in `src/macros.rs` and `examples/macros.rs` (code duplicated between them). `relationize!` generates trie-style nested iterators over a sorted vector (possibly with duplicates - the final value at the leaves of the trie is a number counting the duplicates):

    use dijkstralog::{relationize, iter::Seek};
    let xs: &[(u32, &str)] = &[(1, "one"), (1, "wun")];
    let r = relationize!(xs, u32, &str);
    // Now r is a 2-level-nested Seek-able trie.
    for (num, strings) in r.iter() {
        for (str, count) in strings.iter() {
            println!("{num}, {str} ↦ {count}");
        }
    }

And `nest!` just makes writing nested for loops a little nicer:

    nest! {
        for (a, r_b) in r_ab.iter();
        for (b, ())  in r_b.iter();
        vs.push((a,b));
    };

desugars to:

    for (a, r_b) in r_ab.iter() {
        for (b, ()) in r_b.iter() {
            vs.push((a,b));
        }
    }

