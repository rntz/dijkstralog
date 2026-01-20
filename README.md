This README was written as of 20 January 2026, d4a7cbb45ea3f1b9db4316e5044a972bbf716b8a, and may since be out of date.

# What is this?

An iterator interface and implementations of it that make it easy(-ish) to express worst-case optimal joins. Related to [indexed streams](https://dl.acm.org/doi/abs/10.1145/3591268). The best currently available description of my particular approach is my miniKanren 2025 workshop paper [Fair intersection of seekable iterators](https://arxiv.org/abs/2510.26016v1) and [its talk](https://www.youtube.com/watch?v=-32fwqirjW8).

# Directory & file guide

- `src`: The Rust implementation. TODO DESCRIBE

- `examples`: Example programs. See [Examples](#examples), below.

- `macros`: A subpackage containing some not-yet-working procedural macros for writing Datalog-style queries and rules. See [Macros](#macros), below.

- `minikanren2025`: Stuff for my miniKanren 2025 paper [Fair intersection of seekable iterators](https://arxiv.org/abs/2510.26016v1).

There are various miscellaneous `*.{hs,jl,rkt.py}` files in the root directory. These are mostly experiments with expressing worst-case optimal iterators

# Examples

TODO DESCRIBE

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

