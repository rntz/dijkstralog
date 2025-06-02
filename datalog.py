from dataclasses import dataclass
from typing import Any, Union
import random
from collections import namedtuple

import lftj

def distinct(xs):
    return len(xs) == len(set(xs))

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
Const = int
Term = Union[Var, Const]
Pred = str

# Maps predicates to pairs (arity, sorted_list).
DB = dict[Pred, (int, list)]

Prim = namedtuple('Prim', ['name'])
prims = {x: Prim(x) for x in "eq const".split()}

@dataclass(slots=True, frozen=True)
class Atom:
    head: Union[Pred,Prim]
    args: tuple[Term, ...]
    def __init__(self, head, args):
        object.__setattr__(self, 'head', head)
        object.__setattr__(self, 'args', tuple(args))
    def __str__(self):
        return f"{self.head}({','.join(self.args)})"

# Elaborates a conjunction into a runnable form by:
#
# - eliminating duplicate variables and replacing with Prim('eq').
# - eliminating constants N and replacing with Prim('const').
#
# Returns all fresh variables it created, in some order. This is used to extend
# the variable order.
def elaborate(conjuncts: list[Atom]) -> (list[Var], list[Atom]):
    all_vars = set(x for c in conjuncts for x in c.args if isinstance(x, Var))
    fresh_vars = []
    def gensym(base, sep='.'):
        n = 0
        sym = base
        while sym in all_vars:
            sym = f"{base}{sep}{n}"
            n += 1
        all_vars.add(sym)
        fresh_vars.append(sym)
        return sym

    result = []
    for c in conjuncts:
        args = []
        vars_sofar = set()
        for x in c.args:
            if not isinstance(x, Var): # constants, use const
                value = x
                x = gensym(str(value))
                result.append(Atom(Prim('const'), (value, x)))
            elif x in vars_sofar: # duplicate variable, use eq
                base = x
                x = gensym(base)
                result.append(Atom(Prim('eq'), (base, x)))
            else:
                vars_sofar.add(x)
            args.append(x)

        assert all(isinstance(x, Var) for x in args)
        assert distinct(args)
        result.append(Atom(c.head, args))

    return fresh_vars, result


# ---------- EVALUATION INTO TRIE ITERS ----------
def prim_trie(db: DB, var_order: list[Var], atom: Atom):
    assert isinstance(atom.head, Prim) # precondition
    prim = atom.head.name
    match prim:
        case "eq":
            x,y = atom.args
            assert isinstance(x, Var) and isinstance(y, Var)
            indices = subsequence(atom.args, var_order)
            assert indices is not None, \
                f"Cannot eval eq {x} {y} in var order [{' '.join(var_order)}]"
            return TrieEq(), *indices

        case "const":
            x,y = atom.args
            assert isinstance(x, Const)
            assert isinstance(y, Var)
            return TrieSingle(x), var_order.index(y)

        case _:
            raise ValueError(f"Unrecognized primitive {prim}")

def atom_trie(db: DB, var_order: list[Var], atom: Atom):
    assert distinct(var_order)         # precondition
    # All argument variables are in the variable order somewhere.
    arg_vars = set(v for v in atom.args if isinstance(v,Var))
    missing = arg_vars - set(var_order)
    assert not missing, f"Variable{'s' if len(missing) > 1 else ''} {','.join(missing)} missing from variable order [{' '.join(var_order)}]"

    if isinstance(atom.head, Prim):
        return prim_trie(db, var_order, atom)

    assert isinstance(atom.head, Pred) # only case other than Prim
    # All arguments are variables. TODO: implement this with Const.
    assert all(isinstance(x, Var) for x in atom.args)
    # All arguments are *distinct* variables. TODO: implement this with Eq.
    assert distinct(atom.args)

    arity, table = db[atom.head]
    assert arity == len(atom.args)

    # TODO: I think the best way to handle constant/duplicate arguments is here.
    # we should wrap the SortedListTrie to "project away" the constant/duplicate
    # arguments by seek()ing and failing if we can't find the constant/duplicate.

    # We use a sorting/indexing step if the atom's argument order isn't a
    # subsequence of the variable order.
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
    return lftj.SortedListTrie(arity, table), *indices

# rename this to something else. it duplicates & constant-tests variables. a
# form of indexing without sorting. ordered index?
class AtomTrie(lftj.TrieIter):
    def __init__(self, args, trie: lftj.TrieIter):
        assert len(args) == trie.max_level()
        # I think it should be possible to handle non-grounding views here but I
        # don't want to work out the details yet.
        assert all(x is lftj.Iter for x in trie.levels())

        self.trie = trie
        # Maps variables to their values.
        self.bindings = {}
        # set to True when we fail to find the required constants/duplicate
        # variables.
        self.failed_early = False

        # Build trie levels
        self.var_order = []   # first occurrence of each variable
        self.level_map = []   # maps our levels into trie levels
        self.extras = [[]] # duplicate variables & constants for each self.trie level
        for i, arg in enumerate(args):
            if isinstance(arg, Var) and arg not in self.var_order:
                self.var_order.append(arg)
                self.level_map.append(i)
                self.extras.append([])
            else:
                self.extras[-1].append(arg)
        # Initial prefix should be all constants.
        self.prefix = self.extras.pop(0)
        assert all(isinstance(x,Const) for x in self.prefix)

        # Maps self.trie levels into our levels.
        self.reverse_level_map = [0] * len(self.prefix)
        for i,v in enumerate(self.var_order):
            self.reverse_level_map += [i] * (1 + len(self.extras[i]))

    def levels(self): return [lftj.Iter] * len(self.var_order)
    def level(self):
        l = self.trie.level()
        return self.reverse_level_map[l] if l >= 0 else -1

    def done(self):
        assert 0 <= self.level()
        return self.trie.done() or self.failed_early

    def _enter_extras(self, extras):
        for term in extras:
            value = term if isinstance(term, Const) else self.bindings[term]
            self.trie.enter()
            if self.trie.done() or self.trie.seek(value) or self.trie.key() != value:
                return False    # failure
        return True             # success

    def _backout(self, target_level):
        assert self.level() >= 0
        assert target_level <= self.trie.level()
        for _ in range(target_level, self.trie.level()):
            self.trie.leave()
        assert self.trie.level() == target_level

    def _next_match(self):
        level = self.level()
        assert level >= 0
        var = self.var_order[level]
        extras = self.extras[level]
        var_level = self.level_map[level]
        # we must already be backed out.
        assert self.trie.level() == var_level
        while not self.trie.done():
            # Potential match, need it in self.bindings in case it's in extras.
            self.bindings[var] = self.trie.key()
            if self._enter_extras(extras):
                # match! don't back out, we'll need to be here if enter() is called.
                return True
            # Failed match; keep moving forward.
            self._backout(var_level)
            self.trie.next()
        if var in self.bindings: del self.bindings[var] # just to be safe
        assert self.trie.level() == var_level
        assert self.trie.done()
        return False      # no match after exhausting self.trie at current level

    def enter(self):
        level = self.level()
        assert -1 <= level < self.max_level()
        assert level == -1 or not self.done()

        if level == -1 and not self._enter_extras(self.prefix):
            self.failed_early = True
            return

        self.trie.enter()
        assert self.level() == level+1
        # Find first match for the extra conditions if it exists.
        self._next_match()
        assert self.level() == level+1

    def leave(self):
        level = self.level()
        assert -1 < level <= self.max_level()
        var_level = self.level_map[level]

        self._backout(var_level)
        self.failed_early = False
        self.trie.leave()

        level -= 1
        self._backout(self.level_map[level] if level != -1 else -1)
        assert self.level() == level
        assert level == -1 or self.trie.level() == self.level_map[level]

    def key(self):
        level = self.level()
        assert 0 <= level
        assert not self.done()
        return self.bindings[self.var_order[level]]

    def next(self):
        level = self.level()
        assert level >= 0
        assert not self.done()
        assert not self.trie.done()
        self._backout(self.level_map[level])
        assert not self.trie.done()
        return self.trie.next() or not self._next_match()

    def seek(self, key):
        level = self.level()
        assert level >= 0
        assert not self.done()
        assert not self.trie.done()
        # Exit if we're already at or beyond `key`
        if self.key() >= key: return False

        var_level = self.level_map[level]
        self._backout(var_level)
        self.trie.seek(key)
        raise NotImplementedError # TODO

def conjunct(db: DB, var_order: list[Var], conjuncts: list[Atom]):
    fresh_vars, conjuncts = elaborate(conjuncts)
    # TODO: we're not projecting away these fresh variables before returning
    # them upward! this is bad.
    #
    # TODO: putting fresh_vars at the end seems very wrong! variables standing
    # for constants should go at the HEAD of the variable ordering to be
    # evaluated immediately! And we also want to evaluate equality constraints
    # eagerly, right?
    if fresh_vars:
        var_order += fresh_vars     # don't mutate the list
        # var_order = fresh_vars + var_order
        print(f"Extending variable order to [{' '.join(var_order)}]")
    # TODO: simplify away things that cause problems in atom_trie.
    # alternatively, extend atom_trie to handle them inline.
    return lftj.TrieJoin(*(atom_trie(db, var_order, c) for c in conjuncts))


# ---------- EXAMPLES ----------
global_db = {
    "foo": (2, [("a", 1), ("a", 2),
                ("b", 1), ("b", 2),
                ]),
    "bar": (2, [(1, "one"), (1, "wun"),
                (2, "deux"), (2, "two"),
                ]),
    "baz": (2, [('a', "one"), ('b', "deux"), ("mary", "mary")]),
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
    return list(lftj.trie_iterate(conjunct(db, var_order, query)))

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

query_diag = [Atom("baz", "x x".split())]

tj = conjunct(global_db, 'a b c'.split(), query_triangle)
at = AtomTrie(['x', 1, 'y'], tj)
