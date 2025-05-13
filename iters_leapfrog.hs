-- BUG: draino m is missing 3

-- I think this actually implements leapfrog as described in the LFTJ paper, in
-- that it visits sub-iterators IN ORDER starting with the least-advanced.

{-# LANGUAGE TypeFamilies, FunctionalDependencies, ScopedTypeVariables #-}

import Prelude hiding (head, tail, sum, product)

import Control.Exception (assert)
import Data.Foldable (Foldable (toList, foldMap))
import Data.List (sortBy, insertBy)
import Data.List.NonEmpty (NonEmpty (..), head, tail, uncons, nonEmpty)
import Data.Maybe (mapMaybe, catMaybes)
import Data.Ord (comparing)
import qualified Data.List.NonEmpty as NE

import Control.Monad (forM)
import Control.Monad.State

import Debug.Trace (trace)

-- Applicative without pure. A "semimonoidal functor".
class Functor f => Apply f where
  map2 :: (a -> b -> c) -> f a -> f b -> f c
  pair :: f a -> f b -> f (a,b)
  pair = map2 (,)
  sequence1 :: NonEmpty (f a) -> f (NonEmpty a)
  sequence1 (x :| [])     = fmap NE.singleton x
  sequence1 (x :| (y:ys)) = map2 NE.cons x (sequence1 (y :| ys))
  traverse1 :: (a -> f b) -> NonEmpty a -> f (NonEmpty b)
  traverse1 f xs = sequence1 (f <$> xs)

-- Sorted, seekable iterators as a coinductive type.
data Seek k v = Seek
  -- invariant: lagging <= leading
  { lagging :: !k
  , leading :: !k
  -- `value` should only be accessed when lagging == leading.
  , value :: v
  -- Let t' = search t lo hi. The pre/post conditions for this are:
  -- PRE:  lagging t <= lo               && leading t <= hi
  -- POST:              lo <= lagging t' &&              hi <= leading t'
  , search :: k -> k -> Maybe (Seek k v)
  }

deriving instance Functor (Seek k)

seek :: Ord k => Seek k v -> k -> k -> Maybe (Seek k v)
seek t lo hi | lo < lagging t = trace "lo < lagging" $ Just t
             | hi < leading t = trace "hi < leading" $ Just t -- do we need this?
             | otherwise = search t lo hi

instance Ord k => Apply (Seek k) where
  -- Intersects two seekable iterators.
  map2 f s t = Seek { lagging = lagging s `min` lagging t
                    , leading = leading s `max` leading t
                    , value = f (value s) (value t)
                    , search = if lagging s <= lagging t
                               then advance s t (map2 f)
                               else advance t s (flip (map2 f))
                    }
    where advance s t f lo hi = assert (lagging s <= lagging t) $
                                assert (lagging s <= lo) $
                                assert (leading s `max` leading t <= hi) $
                                do s' <- seek s (lagging t) hi --could change `seek` to `search`
                                   t' <- assert (hi <= leading s') $
                                         seek t (lo `min` lagging s') (leading s')
                                   seek (f s' t') lo (leading t')

-- kinda hacky.
filterMap :: Eq k => (k -> a -> Maybe b) -> Seek k a -> Maybe (Seek k b)
filterMap f t | k == lagging t, Nothing <- v' = filterMap f =<< search t k k
              | otherwise = Just $ t { value = (let Just x = v' in x)
                                     , search = \lo hi -> filterMap f =<< search t lo hi }
  where k = leading t
        v' = f k (value t)


-- Converting in & out of lists
drain :: Eq k => Maybe (Seek k v) -> [(k,v)]
drain Nothing = []
drain (Just s) = [(leading s, value s) | lagging s == leading s]
                 ++ drain (search s (leading s) (leading s))

traceFromSorted :: (Ord k, Show k) => String -> [(k,v)] -> Maybe (Seek k v)
traceFromSorted name [] = Nothing
traceFromSorted name ((k,v):rest) = Just (Seek kt kt v searcher)
  where kt = trace (name ++ " " ++ show k) k
        searcher lo hi = traceFromSorted name $ dropWhile ((< hi) . fst) rest

fromSorted :: Ord k => [(k,v)] -> Maybe (Seek k v)
fromSorted [] = Nothing
fromSorted ((k,v) : rest) = Just $ Seek k k v searcher
  -- We must seek to hi, not lo, because otherwise we won't satisfy our
  -- postcondition that hi <= leading.
  where searcher lo hi = assert (k <= lo && lo <= hi) $
                         fromSorted $ dropWhile ((< hi) . fst) rest


-- Examples
list1 = [(1, "one"), (2, "two"), (3, "three"), (5, "five")]
list2 = [(1, "a"), (3, "c"), (5, "e")]

ms1 = traceFromSorted "A" list1
ms2 = traceFromSorted "B" list2
m = pair <$> ms1 <*> ms2

xs = traceFromSorted "x" [(x,x) | x <- [1,3 .. 100]]
ys = traceFromSorted "y" [(y,y) | y <- [2,4 .. 100]]
zs = traceFromSorted "z" [(z,z) | z <- [1,100]]

mxyz = pair <$> (pair <$> xs <*> ys) <*> zs

-- avoids interleaving of `trace` output with printing of value at REPL
draino x = length xs `seq` xs
  where xs = drain x
