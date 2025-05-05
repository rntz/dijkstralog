from dataclasses import dataclass
from typing import Union
import random

import lftj

# Returns None if xs is not a subsequence of ys.
# Otherwise, returns the index into y for each x in xs.
# This means that ys[subsequence(xs,ys)[i]] = xs[i].
def subsequence(xs, ys):
    i = 0
    indices = []
    for x in xs:
        try: i += ys[i:].index(x)
        except ValueError: return None
        indices.append(i)
        i += 1                  # no free repetitions
    return indices

# strs are variables
# ints are constants
Var = str
Term = Union[Var, int]
Pred = str

# Maps predicates to pairs (arity, sorted_list).
DB = dict[Pred, (int, list)]

@dataclass
class Prim:
    name: str
prims = {x: Prim(x) for x in "eq".split()}

@dataclass
class Atom:
    head: Union[Pred,Prim]
    args: list[Term]

    def __str__(self):
        return f"{self.head}({','.join(self.args)})"

def atom_trie(db: DB, var_order: list[Var], atom: Atom) -> TrieIter:
    # All arguments are variables. TODO: simplify() away this case.
    assert all(isinstance(x, Var) for x in atom.args)
    # All arguments are *distinct* variables. TODO: simplify() away this case.
    assert len(atom.args) == len(set(atom.args))
    # All arguments are in the variable order somewhere.
    assert set(atom.args).issubset(var_order)

    # TODO: handle primitives
    assert not isinstance(atom.head, Prim)
    assert isinstance(atom.head, str)

    arity, table = db[atom.head]
    assert arity == len(atom.args)

    # This feels like it could be simplified.
    indices = subsequence(atom.args, var_order)
    if indices is None:         # not a subsequence, need to re-sort
        print(f"Sorting {atom} to fit [{' '.join(var_order)}]")
        indices, permute = [], []
        for i,v in enumerate(var_order):
            if v not in atom.args: continue
            indices.append(i)
            permute.append(atom.args.index(v))
        table = [tuple(row[j] for j in permute) for row in table]
        table.sort()

    return SortedListTrie(arity, table), *indices

# # Simplifies a conjunction into a runnable form by:
# #
# # - eliminating duplicate variables and replacing with Prim('eq').
# def simplify(conjuncts: list[Atom]) -> list[Atom]:
#     for c in conjuncts:
#         # check disjointness
#         # TODO: handle this using Prim('eq')
#         assert len(set(c.args)) == len(c.args)
#     return conjuncts

def conjunct(db: DB, var_order: list[Var], conjuncts: list[Atom]):
    # TODO: simplify away things that cause problems in atom_trie.
    # alternatively, extend atom_trie to handle them inline.
    return TrieJoin(*(atom_trie(db, var_order, c) for c in conjuncts))


# ---------- EXAMPLES ----------
global_db = {
    "foo": (2, [("a", 1), ("a", 2),
                ("b", 1), ("b", 2),
                ]),
    "bar": (2, [(1, "one"), (1, "wun"),
                (2, "deux"), (2, "two"),
                ]),
    "baz": (2, [('a', "one"), ('b', "deux")]),
}

for arity, data in global_db.values():
    assert all(len(row) == arity for row in data)
    assert list(sorted(data)) == data
    #data.sort()

def run(query, var_order=None, *, db=None):
    global global_db
    if db is None: db = global_db
    if var_order is None:
        var_order = []
        # Pick a variable order from left to right.
        for c in query:
            for v in c.args:
                if not isinstance(v,Var): continue
                if v in var_order: continue
                var_order.append(v)
        print(f"Picking variable order [{' '.join(var_order)}]")
    return list(trie_iterate(conjunct(db, var_order, query)))

query_ab_bc = [
    Atom("foo", ["a", "b"]),
    Atom("bar", ["b", "c"]),
]
expect_ab_bc = [
    ('a', 1, "one"), ('a', 1, "wun"),
    ('a', 2, "deux"), ('a', 2, "two"),
    ('b', 1, "one"), ('b', 1, "wun"),
    ('b', 2, "deux"), ('b', 2, "two"),
]
assert expect_ab_bc == run(query_ab_bc, "a b c".split())

query_triangle = [
    Atom("foo", ["a", "b"]),
    Atom("bar", ["b", "c"]),
    Atom("baz", ["a", "c"]),
]
expect_triangle = [('a', 1, "one"), ('b', 2, "deux")]
assert expect_triangle == run(query_triangle, "a b c".split())
