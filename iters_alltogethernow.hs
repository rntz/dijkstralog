{-# LANGUAGE ScopedTypeVariables #-}

import Prelude hiding (head, tail, sum, product, mapMaybe, filter)

import Control.Exception (assert)
import Debug.Trace (trace)

import Data.Ord (comparing)
import Data.List (sortBy)

-- Applicative without pure. A "semimonoidal functor".
-- In our case, this is actually the Zip typeclass from semialign!
class Functor f => Apply f where
  map2 :: (a -> b -> c) -> f a -> f b -> f c
  pair :: f a -> f b -> f (a,b)
  pair = map2 (,)

-- A lower bound in a totally ordered key-space k; corresponds to some part of an
-- ordered sequence we can seek forward to.
data Bound k = Init | Atleast !k | Greater !k | Done deriving (Show, Eq)
instance Ord k => Ord (Bound k) where
  Init      <= _         = True
  _         <= Done      = True
  _         <= Init      = False
  Done      <= _         = False
  Atleast x <= Atleast y = x <= y
  Atleast x <= Greater y = x <= y
  Greater x <= Atleast y = x <  y
  Greater x <= Greater y = x <= y

-- An iterator has either found a particular key-value pair, or knows a lower
-- bound on future keys.
data Position k v = Found !k v | Bound !(Bound k) deriving (Show, Eq, Functor)

key :: Position k v -> Bound k
key (Found k _) = Atleast k
key (Bound k) = k

data Iter k v = Iter
  { posn :: !(Position k v)
  , seek :: Bound k -> Iter k v
  } deriving Functor

emptyIter :: Iter k v
emptyIter = Iter (Bound Done) (const emptyIter)


-- Converting to & from sorted lists
toSorted :: Iter k v -> [(k,v)]
toSorted (Iter (Bound Done) seek) = []
toSorted (Iter (Bound k) seek) = toSorted $ seek k
toSorted (Iter (Found k v) seek) = (k,v) : toSorted (seek (Greater k))

matches :: Ord k => Bound k -> k -> Bool
matches Init _ = True
matches Done _ = False
matches (Atleast x) y = x <= y
matches (Greater x) y = x <  y

fromSorted :: Ord k => [(k,v)] -> Iter k v
fromSorted [] = emptyIter
fromSorted l@((k,v):_) = Iter (Found k v) seek
  where seek target = fromSorted $ dropWhile (not . matches target . fst) l

traceFromSorted :: (Show k, Ord k) => String -> [(k,v)] -> Iter k v
traceFromSorted name [] = emptyIter
traceFromSorted name l@((k,v):_) = Iter (Found kt v) seek where
  kt = trace (name ++ " " ++ show k) k
  seek target = traceFromSorted name $ dropWhile (not . matches target . fst) l

fromList :: Ord k => [(k,v)] -> Iter k v
traceFromList :: (Show k, Ord k) => String -> [(k,v)] -> Iter k v
fromList = fromSorted . sortBy (comparing fst)
traceFromList name = traceFromSorted name . sortBy (comparing fst)


-- Inner joins, ie generalized intersection.
instance Ord k => Apply (Iter k) where
  map2 f s t = Iter (map2 f (posn s) (posn t)) seek'
    where -- -- The simple implementation without sideways information passing.
          -- seek' k = map2 f (seek s k) (seek t k)
          -- The "leapfrog" implementation.
          seek' k = map2 f s' t'
            where s' = seek s k
                  t' = seek t $ key $ posn s'

-- Think of this as a "value-aware maximum": we find the maximum position, and
-- combine the values if present.
instance Ord k => Apply (Position k) where
  map2 f (Found k1 v1) (Found k2 v2) | k1 == k2 = Found k1 (f v1 v2)
  map2 f x y = Bound (key x `max` key y)


-- Outer joins, ie generalized union.
class Functor f => OuterJoin f where
  outerJoin :: (a -> c) -> (b -> c) -> (a -> b -> c) -> f a -> f b -> f c

-- Think of this as a "value-aware minimum": we find the minimum position, and
-- apply the appropriate function (l, r, b, or nothing) to the values present.
instance Ord k => OuterJoin (Position k) where
  outerJoin l r b p q =
    case key p `compare` key q of
      LT -> l <$> p
      GT -> r <$> q
      EQ -> case (p, q) of
              (Found k x, Found _ y) -> Found k (b x y)
              -- If they're equal but one isn't done finding a value yet, we
              -- have to wait until it does.
              (Bound pos, _) -> Bound pos
              (_, Bound pos) -> Bound pos

instance Ord k => OuterJoin (Iter k) where
  outerJoin l r b s t = Iter (outerJoin l r b (posn s) (posn t))
                             (\k -> outerJoin l r b (seek s k) (seek t k))


-- Converting a function "k -> Maybe v" into an unproductive Iter.
fromFunction :: Ord k => (k -> Maybe v) -> Iter k v
fromFunction f = seek Init
  where seek k = Iter (at k) seek
        at (Atleast k) = case f k of
                           Just v  -> Found k v
                           Nothing -> Bound (Greater k)
        at p = Bound p

filterKey :: Ord k => (k -> Bool) -> Iter k v -> Iter k v
filterKey test s = map2 (\x y -> x) s (fromFunction (\k -> if test k then Just () else Nothing))


-- EXAMPLES
list1 = [(1, "one"), (2, "two"), (3, "three"), (5, "five")]
list2 = [(1, "a"), (3, "c"), (5, "e")]

ms1, ms2 :: Iter Int String
ms1 = traceFromList "A" list1
ms2 = traceFromList "B" list2

m = pair ms1 ms2

xs = traceFromList "x" [(x,x) | x <- [1,3 .. 100]]
ys = traceFromList "y" [(y,y) | y <- [2,4 .. 100]]
zs = traceFromList "z" [(z,z) | z <- [1,100]]

mxyz :: Iter Int ((Int, Int), Int)
mxyz = pair (pair xs ys) zs

-- avoids interleaving of `trace` output with printing of value at REPL
drain x = length xs `seq` xs where xs = toSorted x
