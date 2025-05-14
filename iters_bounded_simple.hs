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

below :: Ord k => k -> Bool -> k -> Bool
below k True  k' = k <= k'
below k False k' = k <  k'

-- Sorted, seekable iterators as a coinductive type.
data Seek k v = Seek
  { key :: !k
  , value :: Maybe v
  -- (seek targ incl) seeks toward the first k such that (below targ incl k).
  -- ie. set incl=True to find first key >= targ
  --     set incl=False to find first key > targ
  , seek :: k -> Bool -> Maybe (Seek k v)
  -- PRECONDITION:      key s <= targ
  -- POSTCONDITION:     below targ incl (key (seek s targ incl))
  -- idempotent: if below targ incl (key s) then seek s targ incl == Just s
  }
deriving instance Functor (Seek k)

instance Ord k => Apply (Seek k) where
  map2 :: Ord k => (a -> b -> c) -> Seek k a -> Seek k b -> Seek k c
  map2 f s t = Seek rk rv rs
    where rk = key s `max` key t
          rv = if key s == key t then f <$> value s <*> value t else Nothing
          rs k incl = do s' <- seek s k incl
                         t' <- seek t (key s') True
                         return $ map2 f s' t'


-- Converting into & out of sorted lists.
drain :: Maybe (Seek k v) -> [(k,v)]
drain Nothing = []
drain (Just s) | Just v <- value s = (key s, v) : drain (seek s (key s) False)
               | otherwise = drain (seek s (key s) True)

fromSorted :: Ord k => [(k,v)] -> Maybe (Seek k v)
fromSorted [] = Nothing
fromSorted l@((k,v):_) = Just $ Seek k (Just v) s
  where s targ inclusive = fromSorted $ dropWhile (not . below targ inclusive . fst) l

traceFromSorted name [] = Nothing
traceFromSorted name l@((k,v):_) = Just $ Seek kt (Just v) s
  where kt = trace (name ++ " " ++ show k) k
        s targ incl = traceFromSorted name $ dropWhile (not . below targ incl . fst) l

fromList :: Ord k => [(k,v)] -> Maybe (Seek k v)
traceFromList :: (Show k, Ord k) => String -> [(k,v)] -> Maybe (Seek k v)
fromList = fromSorted . sortBy (comparing fst)
traceFromList name = traceFromSorted name . sortBy (comparing fst)


-- Examples
list1 = [(1, "one"), (2, "two"), (3, "three"), (5, "five")]
list2 = [(1, "a"), (3, "c"), (5, "e")]

ms1 = traceFromList "A" list1
ms2 = traceFromList "B" list2

m = pair <$> ms1 <*> ms2

xs = traceFromList "x" [(x,x) | x <- [1,3 .. 100]]
ys = traceFromList "y" [(y,y) | y <- [2,4 .. 100]]
zs = traceFromList "z" [(z,z) | z <- [1,100]]

mxyz = pair <$> (pair <$> xs <*> ys) <*> zs

-- avoids interleaving of `trace` output with printing of value at REPL
draino x = length xs `seq` xs
  where xs = drain x

