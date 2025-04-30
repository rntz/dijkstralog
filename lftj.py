import io
import sys
import bisect

# ----- ITERATORS -----
class Iter:
    def done(self): pass        # returns True iff done
    # advances, returns self.done()
    # precondition: not self.done()
    def next(self): pass
    # seeks to first element >= the given key, returns self.done()
    # precondition: not self.done()
    def seek(self, key): pass

    # Implement Python iterator/iterable interface.
    def __iter__(self): return self
    def __next__(self):
        if self.done or self.next(): raise StopIteration
        else: return self.key()

# Wraps a Python iterable into an Iter. seek() is implemented inefficiently.
class Iterate(Iter):
    def __init__(self, iterable):
        self.iter = iter(iterable)
        self.done = False
        self.next()

    def done(self): return self.done
    def key(self): return self.elem

    def next(self):
        assert not self.done
        try: self.elem = next(self.iter)
        except StopIteration: self.done = True
        return self.done

    def seek(self, key):
        assert not self.done
        while self.elem < key:
            if self.next(): return True
        assert key <= self.elem
        return False

# Leapfrog intersection of iterators.
class Leapfrog(Iter):
    def __init__(self, *iters):
        assert len(iters) > 0
        self.iters = list(iters)
        self.finished = any(i.done() for i in iters)
        if not self.finished:
            self.iters.sort(key=lambda x: x.key())
            self.idx = 0
            self.search()

    def done(self):
        return self.finished

    def key(self):
        assert not self.finished
        return self.iters[self.idx].key()

    def rotate(self):
        assert not self.finished
        assert not any(i.done() for i in self.iters)
        self.idx = (self.idx + 1) % len(self.iters)

    # Returns self.done().
    def next(self):
        assert not self.finished
        # FIXME XXX TODO: INCORRECT.
        # 2025-04-28 I don't see how this is wrong?
        self.finished = self.iters[self.idx].next()
        if self.finished: return True
        self.rotate()
        return self.search()

    # Returns self.done().
    def search(self):
        assert not self.finished
        hi = self.iters[self.idx - 1].key()
        while True:
            iter = self.iters[self.idx]
            if iter.key() == hi: return False
            self.finished = iter.seek(hi)
            if self.finished: return True
            hi = iter.key()
            self.rotate()

    # Returns self.done().
    def seek(self, key):
        iter = self.iters[self.idx]
        self.finished = iter.seek(key)
        if self.finished: return True
        self.rotate()
        return self.search()


# ----- TRIE ITERATORS -----
class TrieIter(Iter):
    def leave(self): pass
    # precondition: not self.done()
    def enter(self): pass
    def depth(self): pass       # current depth. initially -1.
    def max_depth(self): pass   # number of levels
    # invariant: -1 <= self.depth() <= self.max_depth()
    # You shouldn't use a TrieIter as a Python iterator.
    def __next__(self): raise NotImplementedError
    def __iter__(self): raise NotImplementedError

class TrieJoin(TrieIter):
    # eg: TrieJoin((x,0,2), (y,1,2), (z,0))
    #
    # This means x,y,z are TrieIters, and
    # we use x at levels 0 & 2
    #        y at levels 1 & 2
    #        z at level  0
    #
    # Level count always starts at 0.
    def __init__(self, *iter_levels):
        # TODO: what if some iterator has 0 levels, i.e. a boolean?
        # TODO: what if there are no iterators?
        maxlevel = max(l for _, *levels in iter_levels for l in levels)
        self.iters = [[] for _ in range(1+maxlevel)]
        self.level = -1         # current depth
        self.frogs = []
        for it, *levels in iter_levels:
            assert it.max_depth() == len(levels)
            assert it.depth() == -1 # shouldn't be open yet
            for l in levels:
                self.iters[l].append(it)

    def depth(self): return self.levelp
    def max_depth(self): return len(self.iters)

    def enter(self):
        assert -1 <= self.level < self.max_depth()
        assert self.level == -1 or not self.done()
        self.level += 1
        for iter in self.iters[self.level]:
            iter.enter()
        self.frogs.append(Leapfrog(*self.iters[self.level]))

    def leave(self):
        assert -1 < self.level <= self.max_depth()
        for iter in self.iters[self.level]:
            iter.leave()
        self.frogs.pop()
        self.level -= 1

    def done(self):
        assert self.level >= 0
        return self.frogs[-1].done()

    def key(self):
        assert self.level >= 0
        assert not self.done()
        return self.frogs[-1].key()

    def next(self):
        assert self.level >= 0 and not self.done()
        return self.frogs[-1].next()

    def seek(self, key):
        assert self.level >= 0
        self.frogs[-1].seek(key)

    def debug_dump(self, name=None, file=None):
        iters = {}
        for lvl in self.iters:
            for it in lvl:
                if id(it) in iters: continue
                iters[id(it)] = it
        all_names = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        assert len(iters) <= len(all_names)
        iter_names = dict(zip(sorted(iters.keys()), all_names))
        if self.level == -1:
            print("inactive")
        else:
            names = (iter_names[id(it)] for it in self.iters[self.level])
            print(f"active {' '.join(names)}")
        for x, it in iters.items():
            it.debug_dump(iter_names[x], file=file)


# An actual trie.
class Trie:
    def __init__(self, data = [], depth = None):
        if depth is None:
            assert data
            depth = len(data[0])
        else:
            assert isinstance(depth, int)
        assert all(len(x) == depth for x in data)
        self.depth = depth
        # If depth == 1, then data maps keys to counts.
        # Otherwise it maps keys to subtries.
        self.data = {}
        for entry in data:
            self.insert(entry)

    def insert(self, key):
        depth = len(key)
        assert depth == self.depth
        assert depth > 0
        k = key[0]
        if depth == 1:
            self.data[k] = self.data.get(k,0) + 1
            return
        trie = self.data.get(k)
        if trie is None:
            trie = self.data[k] = Trie(depth = depth-1)
        trie.insert(key[1:])

    def remove(self, key):
        assert len(key) == self.depth
        k = key[0]
        if k not in self.data: return
        if depth > 1:
            self.data[k].remove(key[1:])
            return
        # Base case, depth=1
        n = self.data.get(k,0)
        if n == 0: return
        elif n == 1: del self.data[k]
        elif n > 1: self.data[k] = n - 1

    def iterator(self): return TrieIterator(self)
    def __iter__(self): return trie_iterate(self.iterator())
    def __repr__(self): return f"Trie({list(self)})"

    def show(self, buf=sys.stdout, prefix=""):
        for key, subtrie in self.data.items():
            if self.depth == 1:
                for _ in range(subtrie): # subtrie is actually a count
                    print(f"{prefix}{key}", file=buf)
            else:
                print(f"{prefix}{key}", file=buf)
                subtrie.show(buf, prefix + "   ")

class TrieIterator(TrieIter):
    def __init__(self, trie: Trie):
        self.trie = trie
        self.stack = None

    def depth(self):
        return len(self.stack) if self.stack is not None else -1

    def max_depth(self): return self.trie.depth

    def done(self):
        assert self.depth() >= 0
        return self.posn >= len(self.keys)

    def key(self):
        assert self.depth() >= 0
        assert not self.done()
        return self.keys[self.posn]

    def next(self):
        assert self.depth() >= 0
        assert not self.done()
        self.posn += 1
        return self.done()

    def seek(self, key):
        assert self.depth() >= 0
        assert not self.done()
        self.posn = bisect.bisect_left(self.keys, key, self.posn)
        return self.done()

    def enter(self):
        if self.depth() == -1:
            self.stack = []
            self.node = self.trie
        else:
            assert not self.done()
            self.stack.append((self.node, self.keys, self.posn))
            self.node = self.node.data[self.key()]
        self.keys = list(self.node.data)
        self.keys.sort()
        self.posn = 0

    def leave(self):
        assert self.depth() >= 0
        if self.stack:
            self.node, self.keys, self.posn = self.stack.pop()
        else:
            self.stack = None


# Trie iterator for a sorted list of tuples
class SortedListTrie(TrieIter):
    # depth = length of the tuples
    # precondition: depth > 0.
    # hm, how does LFTJ handle zero-ary relations??? I guess it doesn't. seems wrong.
    def __init__(self, depth, sorted_list):
        self.tuple_length = depth
        self.sorted_list = sorted_list
        self.bounds = []
        self.region = (0, len(sorted_list)) if sorted_list else None
        assert list(sorted(sorted_list)) == sorted_list
        assert all(len(x) == self.tuple_length for x in sorted_list)

    def depth(self): return len(self.bounds) - 1
    def max_depth(self): return self.tuple_length

    def done(self):
        assert self.depth() >= 0
        return self.region is None

    def key(self):
        assert self.depth() >= 0
        assert not self.done()
        (start, end) = self.region
        assert start != end
        elem = self.sorted_list[start]
        depth = self.depth()
        prefix = elem[:depth+1]
        assert all(prefix == x[:depth+1] for x in self.sorted_list[start:end])
        return elem[depth]

    def next(self):
        depth = self.depth()
        assert depth >= 0
        assert not self.done()
        (start, end) = self.region

        # If we're at the bottom level and our region contains more than one
        # key, we simply advance `start`. This is to handle duplicates correctly
        # (ie bag semantics; if you want set semantics, simply deduplicate the
        # underlying sorted list).
        if depth + 1 == self.tuple_length and start + 1 < end:
            start += 1
            self.region = (start, end)
            return False

        (outer_start, outer_end) = self.bounds[-1]
        assert outer_start <= start <= end <= outer_end
        if end == outer_end: # at end of subtrie; we are done
            self.region = None
            return True

        # Find this next subtrie's extent.
        start = end
        end = bisect.bisect_right(
            self.sorted_list,
            self.sorted_list[start][depth],
            start + 1,
            outer_end,
            key = lambda x: x[depth],
        )
        assert start < end
        self.region = (start, end)
        return False

    def seek(self, key):
        depth = self.depth()
        assert depth >= 0
        assert not self.done()

        (start, end) = self.region
        assert start != end
        # Exit if we're already at `key`.
        current_key = self.sorted_list[start][depth]
        if key == current_key: return False

        (outer_start, outer_end) = self.bounds[-1]
        assert outer_start <= start <= end <= outer_end
        if end == outer_end:
            self.region = None
            return True

        # We only seek() forward.
        assert key > self.sorted_list[start][depth]
        start = bisect.bisect_left(
            self.sorted_list,
            key,
            end,
            outer_end,
            key = lambda x: x[depth],
        )
        if start == outer_end:
            self.region = None
            return True

        end = bisect.bisect_right(
            self.sorted_list,
            self.sorted_list[start][depth],
            start + 1,
            outer_end,
            key = lambda x: x[depth],
        )
        self.region = (start,end)
        return False

    def enter(self):
        depth = self.depth() + 1 # new depth
        assert depth <= self.tuple_length

        if depth == 0:
            length = len(self.sorted_list)
            if not length:
                # the element of bounds should never be inspected since
                # self.done() will be True, but it can get popped by leave().
                self.bounds = [None]
                self.region = None
                return
            outer_start = 0
            outer_end = length
        else:
            assert not self.done()
            (outer_start, outer_end) = self.region
        
        start = outer_start
        key = self.sorted_list[start][depth]
        end = bisect.bisect_right(
            self.sorted_list, key, start + 1, outer_end, key=lambda x: x[depth]
        )
        self.bounds.append((outer_start, outer_end))
        self.region = (start, end)

    def leave(self):
        assert self.depth() >= 0
        self.region = self.bounds.pop()
        if self.depth() == -1:
            length = len(self.sorted_list)
            assert self.region == ((0, length) if length else None)
        else:
            assert not self.done()

    def debug_dump(self, name=None, file=None):
        header = f"{name} " if name is not None else ""
        depth = self.depth()
        state = "    "
        if self.region is None:
            value = "DONE"
        else:
            (start, end) = self.region
            assert start < end
            prefix = self.sorted_list[start][:depth]
            value = f"from {start}-{end} prefix {prefix} keys {self.sorted_list[start]} to {self.sorted_list[end-1]}"
        print(f"{header}{depth:2} {value}", file=file)


# ---------- TESTS ----------
def trie_iterate(iter: TrieIter, debug=False):
    def p(*a,**kw):
        if not debug: return
        print(*a,**kw)
    def debug_dump(*a,**kw):
        if debug: iter.debug_dump(*a,**kw)

    max_depth = iter.max_depth()
    p(f"max_depth: {max_depth}")
    p("entering at ()")
    iter.enter()
    debug_dump()
    if iter.done():
        iter.leave()
        return

    key = ()
    while True:
        assert not iter.done()
        # As long as we're not at the bottom of the trie, descend.
        while len(key) < max_depth - 1 and not iter.done():
            key += (iter.key(),)
            p(f"entering at {key}")
            iter.enter()
            debug_dump()

        while not iter.done():
            entry = key + (iter.key(),)
            p(f"found {entry}")
            yield entry
            p(f"iter.next() after finding {entry}")
            iter.next()
            debug_dump()

        # As long as there are no more keys in this level, ascend.
        while iter.done():
            if key == ():
                iter.leave()
                return
            p(f"leaving to {key}")
            prev_key = key
            key = key[:-1]
            iter.leave()
            debug_dump()
            p(f"calling iter.next() after leaving to {prev_key}")
            iter.next()
            debug_dump()


# ===== SortedListTrie tests =====
examples = [
    (2, [('a', 1), ('b', 2)]),
    # (0, [()]),                  # TODO: this errors.
    # (0, []),                    # TODO: this errors.
    (1, [('a',), ('b',), ('c',)]),
    (1, [('a',), ('a',)]),
    (3, [('a', 2, 10), ('a', 2, 20), ('a', 3, 10), ('b', 1, 10)]),
    # (100, [('a',) * 100, ('a',) * 99 + ('b',), ('b',) * 100]),
    # (sys.maxsize, []),        # enormous depth shouldn't matter
]

for depth, lst in examples:
    lst.sort()
    print(f"Input list:     {lst}")
    result = list(trie_iterate(SortedListTrie(depth, lst)))
    print(f"SortedListTrie: {result}")
    assert lst == result


# ===== LeapFrog tests =====
# TODO


# ===== TrieJoin tests =====
ab = [('a', 1), ('a', 2), ('b', 1), ('b', 2)]
bc = [(1, "one"), (1, "wun"), (2, "deux"), (2, "two")]
ac = [('a', "one"), ('b', "deux")]

# ab JOIN bc = [('a', 1, "one"), ('a', 1, "wun"),
#               ('a', 2, "two"), ('a', 2, "deux"),
#               ('b', 1, "one"), ('b', 1, "wun"),
#               ('b', 2, "two"), ('b', 2, "deux")]

# ab JOIN ac = [('a', 1, "one"), ('a', 2, "one"),
#               ('b', 1, "deux"), ('b', 2, "deux")]

# bc JOIN ac = [('a', 1, "one"), ('b', 2, "deux")]

# ab JOIN bc JOIN ac = [('a', 1, "one"), ('b', 2, "deux")]

abt = SortedListTrie(2, ab)
bct = SortedListTrie(2, bc)
act = SortedListTrie(2, ac)
tj_triangle = TrieJoin((abt, 0, 1), (bct, 1, 2), (act, 0, 2))
assert list(trie_iterate(tj_triangle)) == [('a', 1, "one"), ('b', 2, "deux")]

tj_bc_ac = TrieJoin((bct, 1, 2), (act, 0, 2))
assert list(trie_iterate(tj_bc_ac)) == [('a', 1, "one"), ('b', 2, "deux")]

tj_ab_bc = TrieJoin((abt, 0, 1), (bct, 1, 2))
expect_ab_bc = [
    ('a', 1, "one"), ('a', 1, "wun"),
    ('a', 2, "deux"), ('a', 2, "two"),
    ('b', 1, "one"), ('b', 1, "wun"),
    ('b', 2, "deux"), ('b', 2, "two"),
]
expect_ab_bc.sort()
assert list(trie_iterate(tj_ab_bc)) == expect_ab_bc
