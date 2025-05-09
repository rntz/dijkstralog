{-# LANGUAGE FunctionalDependencies, ScopedTypeVariables #-}
module Iters where

import Prelude hiding (head, tail, sum, product)
import Data.Foldable (Foldable (toList, foldMap))
import Data.List (sortBy, insertBy)
import Data.List.NonEmpty (NonEmpty (..), head, tail, uncons, nonEmpty)
import Data.Maybe (mapMaybe, catMaybes)
import Data.Ord (comparing)
import qualified Data.List.NonEmpty as NE

-- nonempty seekable iterators
class Ord k => Seek1 iter k v | iter -> k, iter -> v where
  key :: iter -> k
  value :: iter -> v
  next :: iter -> Maybe iter
  seek :: k -> iter -> Maybe iter

-- Dumb NonEmpty implementation with inefficient seek.
instance Ord k => Seek1 (NonEmpty (k,v)) k v where
  key = fst . head
  value = snd . head
  next = snd . uncons
  seek k iter = nonEmpty $ dropWhile (\(k',_) -> k' < k) $ NE.toList iter

iterList :: Ord k => [(k,v)] -> Maybe (NonEmpty (k,v))
iterList = nonEmpty . sortBy (comparing fst)

-- Commutative monoid. Laws of `sum`:
-- 0. sum [x] = x
-- 1. sum (concat xss) = sum [sum xs | xs <- xss]       ASSOCIATIVE
-- 2. sum (reverse xs) == sum xs                        COMMUTATIVE
--    or more generally,
--    sum xs == sum ys      if xs is a permutation of ys
class Add v where
  sum :: [v] -> v
  zero :: v
  zero = sum []
  plus :: v -> v -> v
  plus x y = sum [x,y]

-- Monoid, not necessarily commutative. Laws of `product`:
-- 0. product [x] = x
-- 1. product (concat xss) = product [product xs | xs <- xss]   ASSOCIATIVE
class Mul v where
  product :: [v] -> v
  one :: v
  one = product []
  times :: v -> v -> v
  times x y = product [x,y]

-- Adds distributive laws:
-- x `times` sum ys == sum [x `times` y | y <- ys]
-- sum xs `times` y == sum [x `times` y | x <- xs]
class (Add v, Mul v) => Semiring v where

sumOf :: (Add v, Foldable t) => (a -> v) -> t a -> v
sumOf f xs = sum [f x | x <- toList xs]

productOf :: (Mul v, Foldable t) => (a -> v) -> t a -> v
productOf f xs = product [f x | x <- toList xs]

-- newtype Sum a = Sum { getSum :: a }
-- instance Add v => Semigroup (Sum v) where Sum x <> Sum y = Sum (plus x y)
-- instance Add v => Monoid (Sum v) where mconcat = Sum . sum . map getSum
-- newtype Product a = Product { getProduct :: a }
-- instance Mul v => Semigroup (Product v) where Product x <> Product y = Product (times x y)
-- instance Mul v => Monoid (Product v) where mconcat = Product . product . map getProduct


-- The obvious implementation of sorted merge union, generalized to Add.
--
-- Invariant: All iterators in mergeHead are at the same key k. All iterators in
-- mergeRest are strictly after k, and mergeRest is sorted by key.
data Merge iter k v = Merge { mergeHead :: NonEmpty iter, mergeRest :: [iter] }

merge :: (Seek1 iter k v) => [Maybe iter] -> Maybe (Merge iter k v)
merge iters = mergeSorted $ sortBy (comparing key) $ catMaybes iters
-- merge iters = mergeSorted $ sortBy (comparing key) [x | Just x <- iters]

-- precondition: iters are sorted by key
mergeSorted :: Seek1 iter k v => [iter] -> Maybe (Merge iter k v)
mergeSorted iters = do
  it :| its <- nonEmpty iters
  let k = key it
  let (pre, post) = span ((k ==) . key) its
  return $ Merge (it :| pre) post

instance (Add v, Seek1 iter k v) => Seek1 (Merge iter k v) k v where
  key (Merge (h :| _) _) = key h
  value (Merge hds _) = sumOf value hds

  seek k (Merge hds rest) = merge $ map (seek k) $ NE.toList hds ++ rest

  next (Merge hds rest) = do
    -- Drop the first element from each head, throwing away empty iterators.
    let hds' = mapMaybe next $ NE.toList hds
    -- Insert hds' into rest, preserving sorted order.
    let iters = foldr (insertBy (comparing key)) rest hds'
    mergeSorted iters


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
  length (Zip lsRev cur post) = 1 + length lsRev + length post

rotate :: Zip a -> Zip a
rotate (Zip lsRev x (r:rs)) = Zip (x:lsRev) r rs
rotate (Zip lsRev x []) = Zip [] x' rs'
  where x' :| rs' = NE.reverse (x :| lsRev)

-- Invariant: in (Leapfrog its), all iterators in its have the same key.
newtype Leapfrog it k v = Leapfrog (Zip it) deriving Show

search :: forall it k v. (Mul v, Seek1 it k v) => k -> Zip it -> Maybe (Leapfrog it k v)
search k z = loop k z count
  where
    count = length z
    loop :: k -> Zip it -> Int -> Maybe (Leapfrog it k v)
    loop k z 0 = Just (Leapfrog z)
    loop k z n = do
      i <- seek k (current z)
      let k' = key i
      let z' = rotate (z { current = i })
      let n' = if k == k' then n-1 else count
      loop k' z' n'

-- We don't do the sort-by-min-key trick in the paper; our search doesn't need it.
leapfrogInit :: (Mul v, Seek1 it k v) => NonEmpty (Maybe it) -> Maybe (Leapfrog it k v)
leapfrogInit xs = do
  it :| its <- sequence xs
  -- no point in rotating, it won't reduce the # of seeks/comparisons; we'd need
  -- a searchAllButOne function instead.
  search (key it) (Zip [] it its)

instance (Mul v, Seek1 iter k v) => Seek1 (Leapfrog iter k v) k v where
  key (Leapfrog z) = key (current z)
  value (Leapfrog z) = productOf value z
  next (Leapfrog z) = do
    it <- next (current z)
    search (key it) (z { current = it })
  seek k l@(Leapfrog z)
    | k <= key l = Just l -- avoids N = (length z) comparisons
    | otherwise  = search k z

-- simple example
i1 = iterList [(1, "a"), (3, "c"), (5, "e")]
i2 = iterList [(1, "one"), (2, "two"), (3, "three")]
i3 = iterList [(1, "uno"), (2, "dos"), (3, "tres")]
-- Just lf = leapfrogInit (i1 :| [i2,i3])


-- Trie join as nested leapfrog intersection? Not obvious how to do it!
--
-- I guess what I'm trying to do is roughly:
--
--     NonEmpty (Maybe (Iter k1 (Maybe (Iter k2 v))))
--  ~> Maybe (Leapfrog k1 (NonEmpty (Maybe (Iter k2 v))))
--  ~> Maybe (Leapfrog k1 (Maybe (Leapfrog k2 v)))
--
-- Letting
--
--     MI = Maybe . Iter
--     ML = Maybe . Leapfrog
--
-- This is
--
--     NonEmpty (MI k1 (MI k2 v))
--  ~> ML k1 (NonEmpty (MI k2 v))
--  ~> ML k1 (ML k2 v)
--
-- IE. I'm pushing NonEmpty over MI, turning it into ML. Observe that
--
--     leapfrogInit :: NonEmpty (MI k v) -> ML k v
--
-- So, hm!
