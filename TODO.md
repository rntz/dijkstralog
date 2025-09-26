# TODOs for dijkstralog

1. Parallel splitting of seekable iterators.
2. Incorporate perf insights from experiments in minikanren2025/ directory.

# 1. Parallel splitting

`trans2_parallel.rs` nets a big speedup from parallelism by partitioning the delta and processing each partition in parallel.

It's obvious how to split a single relation. But how do we "partition" an inner join? Here's one obvious way (based on [Baeza-Yates intersection][baeza-yates] [1]): split the smallest relation in half, finding its median `x`, and partition each other relation on `x`. If more parallelism is desired, recursively partition each half, and so on.

It's not *too* hard to see how to do this for seekable iterators. One simple way to do it (though with some redundant work) is two methods, `split_median()` and `split_at()`. `split_at(x)` splits an iterator into two iterators, partitioned at the given key `x`. It's obvious how to implement this for sorted lists and inner joins. `split_median()` returns (a) the size of the smallest relation (b) the median of the smallest relation and (c) two iterators, partitioned at the median. It's obvious how to implement this for sorted lists; for inner joins, we can (somewhat redundantly, alas) call `split_median()` on each half, pick the one with the smaller size, and then split the larger one at the median of the smaller. There's probably some way to do this with less redundancy.

[1] Ricardo Baeza-Yates and Alejandro Salinger, 2010. "Fast Intersection Algorithms for Sorted Sequences".

[baeza-yates]: https://www.researchgate.net/publication/221349779_Fast_Intersection_Algorithms_for_Sorted_Sequences

# 2. Perf insights

See `minikanren2025/optimization-notes.org`. The main point is that inlining `posn()` into `seek()` so that `seek()` returns the current position speeds things up.

