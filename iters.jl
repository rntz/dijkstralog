# ----- NEXT STEPS -----
#
# 1 extension of nested iterators to more arguments
#   R'(a,x,b,y) = R(a,b)
#
# 2 duplication of variables,
#   R'(a) = R(a,a)
#
# 3 constants,
#   R'(a,b) = R(a,5,b)
#
# 4 build nested iterators from a sorted array
#
# 5 macros to translate Datalog syntax into iterator operations
#
# ----- THINGS THAT ARE HARD -----
#
# 1 Projection of middle variables probably requires materialization?
#
#     R'(b) = Σa. R(a,b)
#
#   Because we need to union/sum all the sub-iterators R(a,_).
#   There are dynamically many of these.
#   We could allocate and do a massively wide merge???
#   We'd need a priority/sorted queue to avoid seeking *every* sub-iterator, though;
#   instead, cache positions, always seek the lowest iterator, and stop once the
#   lowest matches the given bound.
#   (This is an interesting strategy! Is it useful, though?)
#
# 1a
#
#   Projections from the end, however, can be done with minimal state by just
#   accumulating into a semiring value:
#
#     R'(a) = Σb. R(a,b)
#
#   And in the Boolean case, we even get short-circuiting!
#
#     R'(a,b) = Σc,d,e. R(a,b,c,d,e)
#
#   Because for fixed a,b we just need to check whether {x,y,z : R(a,b,x,y,z) }
#   is nonempty. Of course, this requires R be able to ground c,d,e.

module Iters

struct Init end
struct Done end
struct Atleast{K}
    key::K
end
struct Greater{K}
    key::K
end

const Bound{K} = Union{Init, Done, Atleast{K}, Greater{K}}

# Total ordering on Bounds
Base.isless(_::Init,  _::Init)  = false
Base.isless(_::Bound, _::Init)  = false
Base.isless(_::Init,  _::Bound) = true

Base.isless(_::Done,  _::Done)  = false
Base.isless(_::Done,  _::Bound) = false
Base.isless(_::Bound, _::Done)  = true

Base.isless(x::Atleast, y::Atleast) = isless(x.key, y.key)
Base.isless(x::Greater, y::Greater) = isless(x.key, y.key)
Base.isless(x::Greater, y::Atleast) = isless(x.key, y.key)
Base.isless(x::Atleast, y::Greater) = x.key <= y.key

function matches(x::Bound{K}, y::K) where {K}
    x <= Atleast(y)
end

struct Found{K,V}
    key::K
    value::V
end

const Position{K,V} = Union{Found{K,V}, Bound{K}}

to_bound(x::Bound) = x
to_bound(x::Found) = Atleast(x.key)


# ---------- SEEKABLE ITERATORS ----------
# A Seek subclass should implement:
# posn(Seek) -> Position
# seek(Seek, Bound) -> Nothing (mutates the Seek argument)
abstract type Seek end

function position(iter::Seek)
    throw("No implementation of position() found")
end

function seek(iter::Seek, bound::Bound)
    throw("No implementation of seek() found")
end

# TODO: implement something that converts a Seek into a standard Julia iterator
function to_sorted(iter::Seek, _::Type{K}, _::Type{V}) where {K,V}
    elems = Tuple{K,V}[]
    while true
        posn = position(iter)
        (posn isa Done) && return elems
        if posn isa Found
            push!(elems, (posn.key, posn.value))
            posn = Greater(posn.key)
        end
        seek(iter, posn)
    end
end

to_sorted(iter::Seek) = to_sorted(iter, Any, Any)
from_sorted(v::Vector{Tuple{K,V}}) where {K,V} = SortedSeek(v)


# ---------- SORTED ARRAYS ----------
mutable struct SortedSeek{K,V} <: Seek
    const array::Vector{Tuple{K,V}}
    index::Int

    SortedSeek(array::Vector{Tuple{K,V}}) where {K,V} =
        new{K,V}(array, firstindex(array))
end

function position(s::SortedSeek)
    @assert firstindex(s.array) <= s.index <= 1 + lastindex(s.array)
    s.index <= lastindex(s.array) || return Done()
    return Found(s.array[s.index]...)
end

function seek(s::SortedSeek{K,V}, bound::Bound{K}) where {K,V}
    @assert firstindex(s.array) <= s.index <= 1 + lastindex(s.array)
    # Exit early if already Done.
    s.index <= lastindex(s.array) || return
    # Exit early if we already match the bound.
    # This is necessary to ensure the preconditions of bisect(), below.
    current_key = first(s.array[s.index])
    matches(bound, current_key) && return
    # "Bump" optimization. Should test whether this is worth it.
    # Perhaps should generalize to "test next element".
    if bound isa Greater && bound.key == current_key
        s.index += 1
        return
    end
    # General case: binary search.
    (lo, hi) = bisect(s.index, 1 + lastindex(s.array)) do i
        matches(bound, first(s.array[i]))
    end
    s.index = hi                # first index matching bound
    return
end

# Binary search, adapted from https://byorgey.wordpress.com/2023/01/01/competitive-programming-in-haskell-better-binary-search/
# precondition: test(lo) = false, test(hi) = true
# returns (x,y) such that test(x) = false, test(y) = true and x + 1 == y
#
# Julia's searchsortedfirst() is not expressive enough; I cannot get it to do
# what I need even using the `lt` and `by` parameters.
function bisect(test, lo, hi)
    while lo + 1 < hi           # while there is room between lo & hi to search...
        mid = div(lo + hi, 2)   # find the midpoint...
        if test(mid)            # and test it to determine which half to narrow to
            hi = mid            # search (lo,mid) b/c test(lo) = false, test(mid) = true
        else
            lo = mid            # search (mid,hi) b/c test(mid) = false, test(hi) = true
        end
    end
    return (lo, hi)
end


# ---------- INNER JOINS ----------
struct InnerJoin{T <: Tuple{Seek, Vararg{Seek}}} <: Seek
    iters::T
    InnerJoin(x...) = new{typeof(x)}(x)
end

function position(join::InnerJoin)
    @assert !isempty(join.iters)
    posns = map(position, join.iters)
    p = first(posns)
    if all(q -> q isa Found && p.key == q.key, posns)
        return Found(p.key, map(p -> p.value, posns))
    end
    return maximum(map(to_bound, posns))
end

function seek(join::InnerJoin, bound::Bound)
    for iter in join.iters
        seek(iter, bound)
        bound = to_bound(position(iter))
    end
    return
end


# ---------- MAPPING ----------
struct Map{T <: Seek, F <: Function} <: Seek
    iter::T
    func::F
end

Base.map(f, p::Found) = Found(p.key, f(p.value))
Base.map(f, p::Bound) = p

position(iter::Map) = map(iter.func, position(iter.iter))
seek(iter::Map, bound::Bound) = seek(iter.iter, bound)

Base.map(f, t::Seek) = Map(t, f)
Base.map(f, t::Seek...) = Map(InnerJoin(t...), x -> f(x...))


# ---------- MERGE (UNIFORM OUTER JOIN) ----------
#
# An outer join where we treat the values of sub-iterators uniformly, without
# regard to their index in our tuple of sub-iterators, so we can just ignore
# those that lack have a value for a particular key.
struct Merge{F <: Function, T <: Tuple{Seek, Vararg{Seek}}} <: Seek
    func::F
    iters::T
end

function position(join::Merge)
    posns = map(position, join.iters)
    bound = minimum(map(to_bound, posns))
    bound isa Atleast || return bound
    minima = filter(p -> to_bound(p) == bound, posns)
    any(p -> p isa Bound, minima) && return bound
    return Found(bound.key, join.func(map(p -> p.value, minima)...))
end

function seek(join::Merge, bound::Bound)
    for it in join.iters
        seek(it, bound)
    end
end


# ---------- SEMIRING SEMANTICS ----------
Base.:+(x::Seek...) = Merge(Base.:+, x)
Base.:*(x::Seek...) = map(Base.:*, x...)


# ---------- EXAMPLES ----------
vec  = [(1, "one"), (2, "two"), (3, "three")]
vec2 = [(1, "uno"), (3, "tres"), (5, "cinco")]

mks() = from_sorted(vec)
mks2() = from_sorted(vec2)
mkj() = InnerJoin(from_sorted(vec), from_sorted(vec2))

s  = mks()
s2 = mks2()
j  = mkj()

counts1 = [("alice", 2), ("bob", 1)]
counts2 = [("bob", 10), ("charlie", 20)]

mkc1() = from_sorted(counts1)
mkc2() = from_sorted(counts2)

end
