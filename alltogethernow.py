import bisect
from dataclasses import dataclass

class Bound:
    def to_bound(self): return self
    def __lt__(self, other):
        return self <= other and self != other
    def __le__(self, other):
        if isinstance(self, Init) or isinstance(other, Done): return True
        if isinstance(self, Done) or isinstance(other, Init): return False
        assert isinstance(self, Atleast|Greater)
        assert isinstance(other, Atleast|Greater)
        if isinstance(self, Greater) and isinstance(other, Atleast):
            return self.key < other.key
        return self.key <= other.key

@dataclass
class Atleast(Bound):
    key: object
@dataclass
class Greater(Bound):
    key: object
@dataclass
class Init(Bound): pass
@dataclass
class Done(Bound): pass

@dataclass
class Found:
    key: object
    value: object
    def to_bound(self): return Atleast(self.key)

def to_dict(it): return dict(drain(it))
def to_list(it): return list(drain(it))
def drain(it):
    target = None
    while target != Done():
        posn = it.send(target)
        target = posn
        if isinstance(posn, Found):
            yield posn.key, posn.value
            target = Greater(posn.key)

def seek_list(elems: list[tuple[object,object]]):
    length = len(elems)
    index = 0
    while index < length:
        key, value = elems[index]
        posn = Found(key,value)
        target = yield posn
        if target == Greater(key): # bump optimization
            index += 1
        else:
            index = bisect.bisect_left(elems, target, index, length,
                                       key=lambda x: Atleast(x[0]))
    while True: yield Done()

nums = [ (1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five") ]
t = seek_list(nums)

def iter_map(f, *iters):
    assert len(iters)           # don't handle zero-ary case yet.
    posns = [next(it) for it in iters]
    while True:
        posn = max(p.to_bound() for p in posns)
        if all(p.to_bound() == posn and isinstance(p, Found) for p in posns):
            posn = Found(posn.key, f(*(p.value for p in posns)))

        target = yield posn
        posns = []
        for it in iters:
            posn = it.send(target)
            posns.append(posn)
            target = posn.to_bound()

# We supply `default` as argument for iterators which are missing the key.
def iter_outer_join(f, *iters, default=None):
    posns = [next(it) for it in iters]
    while any(p != Done() for p in posns):
        bounds = [p.to_bound() for p in posns]
        posn = min(bounds)
        vals = []
        for b,p in zip(bounds,posns):
            if b != posn:
                vals.append(default)
                continue
            if not isinstance(p, Found):
                break
            vals.append(p.value)
        else:
            assert isinstance(posn, Atleast)
            print(f"{vals=}")
            assert len(vals) == len(iters)
            posn = Found(posn.key, f(*vals))

        target = yield posn
        posns = [it.send(target) for it in iters]

    while True: yield Done()
