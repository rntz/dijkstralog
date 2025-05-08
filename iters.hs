{-# LANGUAGE FunctionalDependencies, ScopedTypeVariables #-}
module Iters where

import Prelude hiding (head, tail)
import Data.List (sortBy, sortOn)
import Data.List.NonEmpty (NonEmpty (..), head, tail, uncons)
import qualified Data.List.NonEmpty as NE
import Data.Foldable (Foldable (..))

-- nonempty seekable iterators
class Ord k => Seek1 iter k v | iter -> k, iter -> v where
  keyValue :: iter -> (k, v)
  keyOf :: iter -> k
  valueOf :: iter -> v
  keyOf = fst . keyValue
  valueOf = snd . keyValue
  next :: iter -> Maybe iter
  seek :: k -> iter -> Maybe iter

-- Dumb NonEmpty implementation with inefficient seek.
instance Ord k => Seek1 (NonEmpty (k,v)) k v where
  keyValue = head
  next = snd . uncons
  seek key iter = NE.nonEmpty $ dropWhile (\(k,_) -> k < key) $ NE.toList iter

iterList :: Ord k => [(k,v)] -> Maybe (NonEmpty (k,v))
iterList = NE.nonEmpty . sortOn fst


{-
THIS VERSION AVOIDS THE USE OF ZIPPERS BUT DOES NOT ROTATE THROUGH ITERATORS
LIKE LEAPFROG FROM THE LFTJ PAPER DOES. INSTEAD IT STARTS SEARCHING FROM THE
FIRST ITERATOR EACH TIME.
-}

-- -- Invariant: [it] is nonempty and all iterators are at same key.
-- data Leapfrog it k v = Leapfrog k v [it]

-- rotate :: Int -> [a] -> [a]
-- rotate n xs = drop n xs ++ take n xs

-- -- Returns iterators in the same order it was handed them.
-- -- TODO: use quickcheck to test this.
-- -- am I hitting every case?
-- search :: Seek1 it k v => k -> [it] -> Maybe (k, [it])
-- search key iters = do (n, k, its) <- loop 0 key iters []
--                       return (k, rotate n its)
--   where loop n key [] dones = Just (n, key, reverse dones)
--         loop n key (it_old:its) dones = do
--           it <- seek key it_old
--           let (k,v) = get it
--           if key == k
--           then loop (n+1) k its (it : dones)
--           else loop (n+1) k (its ++ reverse dones) [it]


-- A list zipper.
data Zip a = Zip { preRev :: [a], current :: a, post :: [a] } deriving Show
instance Foldable Zip where
  toList (Zip lsRev cur post) = reverse lsRev ++ cur : post
  foldMap f z = foldMap f (toList z)

rotate :: Zip a -> Zip a
rotate (Zip lsRev x (r:rs)) = Zip (x:lsRev) r rs
rotate (Zip lsRev x []) = Zip [] x' rs'
  where x' :| rs' = NE.reverse (x :| lsRev)

-- Invariant: in (Leapfrog k v its),
-- (and [k == keyOf it | it <- its] && v == foldMap valueOf its).
data Leapfrog it k v = Leapfrog k v (Zip it) deriving Show

search :: (Monoid v, Seek1 it k v) => k -> Zip it -> Maybe (Leapfrog it k v)
search key z = loop key z count
  where
    count = length (preRev z) + length (post z)
    loop k z 0 = Just (Leapfrog k (foldMap valueOf z) z)
    loop k z n = do
      i <- seek k (current z)
      let k' = keyOf i
      let z' = rotate (z { current = i })
      let n' = if k == k' then n-1 else count
      loop k' z' n'

-- We don't do the sort-by-min-key trick in the paper; our search doesn't need it.
leapfrogInit :: (Monoid v, Seek1 it k v) => NonEmpty (Maybe it) -> Maybe (Leapfrog it k v)
leapfrogInit xs = do
  it :| its <- sequence xs
  search (keyOf it) (rotate (Zip [] it its))

instance (Monoid v, Seek1 iter k v) => Seek1 (Leapfrog iter k v) k v where
  keyValue (Leapfrog k v _) = (k,v)
  next (Leapfrog _ _ z) = do
    it <- next (current z)
    search (keyOf it) (z { current = it })
  seek key l@(Leapfrog k v z)
    | key <= k  = Just l -- avoids N = (length z) comparisons
    | otherwise = search key z

-- simple example
i1 = iterList [(1, "a"), (3, "c"), (5, "e")]
i2 = iterList [(1, "one"), (2, "two"), (3, "three")]
i3 = iterList [(1, "uno"), (2, "dos"), (3, "tres")]
Just lf = leapfrogInit (i1 :| [i2,i3])


-- SEMIRING SEMANTICS TIME?
