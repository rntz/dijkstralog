{-# LANGUAGE TypeFamilies, FunctionalDependencies, ScopedTypeVariables, PartialTypeSignatures #-}

import Prelude hiding (head, tail, sum, product, mapMaybe)

import Control.Exception (assert)
import Data.Coerce (coerce)
import Data.Foldable (Foldable (toList, foldMap))
import Data.List (sortBy, insertBy)
import Data.List.NonEmpty (NonEmpty (..), head, tail, uncons, nonEmpty)
import Data.Ord (comparing)
import qualified Data.List.NonEmpty as NE

import Debug.Trace (trace)

-- Applicative without pure. A "semimonoidal functor".
-- In our case, this is actually the Zip typeclass from semialign!
class Functor f => Apply f where
  map2 :: (a -> b -> c) -> f a -> f b -> f c
  pair :: f a -> f b -> f (a,b)
  pair = map2 (,)

data Bound = AtLeast | Greater deriving (Show, Eq, Ord)

matches :: Ord k => Bound -> k -> k -> Bool
matches AtLeast k k' = k <= k'
matches Greater k k' = k  < k'

data Position k v = At !k (Maybe v) | Done deriving (Show, Eq, Functor)

-- Sorted, seekable iterators.
data Iter k v = Iter
  { posn :: !(Position k v)
  -- (seek b tgt) seeks toward the first k such that (matches b tgt k).
  -- ie. let b = AtLeast to find first key >= tgt
  --     let b = Greater to find first key  > tgt
  , seek :: Bound -> k -> Iter k v
  --  PRECONDITION: key s <= tgt
  -- POSTCONDITION: matches b tgt (key (seek s b tgt))
  -- idempotent: if below targ incl (key s) then seek s targ incl == Just s
  } deriving Functor


-- Inner joins, ie generalized intersection.

-- Think of this as "value-aware maximum": we find the maximum among two
-- Positions, combining their values as appropriate.
instance Ord k => Apply (Position k) where
  map2 f Done      _         = Done
  map2 f _         Done      = Done
  map2 f (At k1 v) (At k2 u) = At (max k1 k2) value
    where value | k1 == k2  = f <$> v <*> u
                | otherwise = Nothing

instance Ord k => Apply (Iter k) where
  map2 :: Ord k => (a -> b -> c) -> Iter k a -> Iter k b -> Iter k c
  map2 f s t = Iter rp rs where
    rp = map2 f (posn s) (posn t)
    -- Simple definition.
    -- rs b k = map2 f (seek s b k) (seek t b k)
    -- Leapfrog definition.
    rs b k | At k' _ <- posn s' = map2 f s' (seek t AtLeast k')
           | otherwise = empty
      where s' = seek s b k
            empty = Iter Done (\_ _ -> empty)


-- Outer joins, ie generalized union.
-- Do we have a left thing, a right thing, or both?
--
-- This section rediscovers These/Semialign/Align from Data.Align
-- https://hackage.haskell.org/package/semialign-1.3.1/docs/Data-Align.html#v:align
-- https://hackage.haskell.org/package/semialign-1.3.1
--
-- I think (Crosswalk List) might give us List-ary union for free if we
-- implement (Align (Seek k v))?
data LR x y = L x | R y | B x y -- These

lr :: (a -> c) -> (b -> c) -> (a -> b -> c) -> LR a b -> c
lr l r b (L x) = l x
lr l r b (R y) = r y
lr l r b (B x y) = b x y

class Functor f => OuterJoin f where -- Semialign
  empty :: f a
  outerPair :: f a -> f b -> f (LR a b)
  outerJoin :: (LR a b -> c) -> f a -> f b -> f c
  outerJoin f xs ys = f <$> outerPair xs ys
  -- TODO: Laws.

instance OuterJoin Maybe where
  empty = Nothing
  -- -- Concise outerPair implementation:
  -- outerPair = maybe (R <$>) (\x -> Just . maybe (L x) (B x))
  -- An explicit implementation is clearer pedagogically:
  outerPair (Just x) (Just y) = Just $ B x y
  outerPair (Just x) Nothing  = Just $ L x
  outerPair Nothing  (Just y) = Just $ R y
  outerPair Nothing  Nothing  = Nothing

-- Think of this as a "value-aware minimum" operation: we're finding the minimum
-- among two positions, combining their values as appropriate for an outer join.
instance Ord k => OuterJoin (Position k) where
  empty = Done
  outerPair Done q    = R <$> q
  outerPair p    Done = L <$> p
  outerPair (At k1 v1) (At k2 v2) =
    case compare k1 k2 of
      EQ -> At k1 (B <$> v1 <*> v2)
      LT -> At k1 (L <$> v1)
      GT -> At k2 (R <$> v2)

instance Ord k => OuterJoin (Iter k) where
  empty = Iter Done (\_ _ -> empty)
  outerPair s t = Iter { posn = outerPair (posn s) (posn t)
                       , seek = \b k -> outerPair (seek s b k) (seek t b k) }

-- Union via outer join: we combine the values using a commutative semigroup
-- operator. (If it's not commutative that's fine too, I guess? But make sure
-- you know what you're doing.)
unionWith :: Ord k => (v -> v -> v) -> Iter k v -> Iter k v -> Iter k v
unionWith f = outerJoin (lr id id f)

-- More generally: (Semigroup v, Semialign f) => Semigroup (f v)?
-- and             (Semigroup v, Align f)     => Monoid (f v)?
instance (OuterJoin f, Semigroup a) => Semigroup (f a) where
  (<>) = outerJoin (lr id id (<>)) --this is salign!

instance (OuterJoin f, Semigroup a) => Monoid (f a) where mempty = empty


-- Converting into & out of sorted lists.
toSorted :: Iter k v -> [(k,v)]
toSorted (Iter Done _) = []
toSorted (Iter (At k vopt) seek)
  | Just v <- vopt = (k, v) : toSorted (seek Greater k)
  | otherwise = toSorted (seek AtLeast k)

fromSorted :: Ord k => [(k,v)] -> Iter k v
fromSorted l = Iter p s where
  p | (k,v):_ <- l = At k (Just v)
    | otherwise    = Done
  s b tgt = fromSorted $ dropWhile (not . matches b tgt . fst) l

traceFromSorted name l = Iter p s where
  p | (k,v):_ <- l = At (trace (name ++ " " ++ show k) k) (Just v)
    | otherwise    = Done
  s b tgt = traceFromSorted name $ dropWhile (not . matches b tgt . fst) l

fromList :: Ord k => [(k,v)] -> Iter k v
traceFromList :: (Show k, Ord k) => String -> [(k,v)] -> Iter k v
fromList = fromSorted . sortBy (comparing fst)
traceFromList name = traceFromSorted name . sortBy (comparing fst)


-- Examples
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

