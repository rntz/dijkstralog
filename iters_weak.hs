{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies, FunctionalDependencies, ScopedTypeVariables #-}
module Iters where

import Prelude hiding (head, tail, sum, product)
import Data.Foldable (Foldable (toList, foldMap))
import Data.List (sortBy, insertBy)
import Data.List.NonEmpty (NonEmpty (..), head, tail, uncons, nonEmpty)
import Data.Maybe (mapMaybe, catMaybes)
import Data.Ord (comparing)
import qualified Data.List.NonEmpty as NE

-- data Q t k v = Found k v t | Bound k k t

-- nonempty seekable iterators.
class Ord (Key iter) => Seek iter where
  type Key iter
  type Value iter
  seek :: Key iter -> iter -> Maybe (Q iter)

type ShowKV it = (Show it, Show (Key it), Show (Value it))

data Q a = Found (Key a) (Value a) a
         | Bound (Key a) (Key a)   a

bounds :: Q a -> (Key a, Key a)
bounds (Bound lo hi _) = (lo, hi)
bounds (Found k _ _) = (k, k)

loBound = fst . bounds
hiBound = snd . bounds

deriving instance ShowKV a => Show (Q a)

-- dumb implementation for sorted lists with inefficient seek
newtype SortSeek k v = SortSeek [(k,v)]

instance Ord k => Seek (SortSeek k v) where
  type Key (SortSeek k v) = k
  type Value (SortSeek k v) = v

  seek tgt (SortSeek []) = Nothing
  seek tgt (SortSeek ((k,v) : kvs))
    | tgt <= k = Just (Found k v (SortSeek kvs))
    | otherwise = seek tgt (SortSeek kvs)


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

-- data Leap2 t u = Leap2
--   { left :: t
--   , rght :: u
--   , hiLeft :: Bool --is the hi bound in left?
--   , loLeft :: Bool --is the lo bound in left?
--   }

-- deriving instance (ShowKV it1, ShowKV it2) => Show (Leap2 it1 it2)

-- leap2 :: (Seek t, Seek u, Key t ~ Key u) => Q t -> Q u -> Q (Leap2 t u)
-- leap2 (Found k1 v1 r1) (Found k2 v2 r2)
--   -- TODO
--   | k1 == k2 = Found k1 (v1,v2) (Leap2 r1 r2 undefined)

-- instance (Seek it1, Seek it2, Key it1 ~ Key it2) => Seek (Leap2 it1 it2) where
--   type Key (Leap2 it1 it2) = Key it1
--   type Value (Leap2 it1 it2) = (Value it1, Value it2)
--   seek target (Leap2 l r inl) = undefined --TODO
