{-# LANGUAGE TypeFamilies, FunctionalDependencies, ScopedTypeVariables #-}

import Prelude hiding (head, tail, sum, product)

import Control.Exception (assert)
import Data.Foldable (Foldable (toList, foldMap))
import Data.List (sortBy, insertBy)
import Data.List.NonEmpty (NonEmpty (..), head, tail, uncons, nonEmpty)
import Data.Maybe (mapMaybe, catMaybes)
import Data.Ord (comparing)
import qualified Data.List.NonEmpty as NE

import Debug.Trace (trace)

-- Seekable iterators as a coinductive type.
data Seek k v = Seek
  { lo :: !k
  , hi :: !k
  -- `value` and `bump` should only be accessed when lo == hi.
  , value :: v
  , bump :: Maybe (Seek k v)
  , seek :: k -> Maybe (Seek k v)
  }

instance (Eq k, Show k, Show v) => Show (Seek k v) where
  show s = "{lo = " ++ show (lo s)
           ++ ", hi = " ++ show (hi s)
           ++ (if lo s == hi s then ", value = " ++ show (value s) else "")
           ++ "}"

drain :: Eq k => Maybe (Seek k v) -> [(k,v)]
drain Nothing = []
drain (Just s)
  | lo s == hi s = (lo s, value s) : drain (bump s)
  | otherwise    = drain (seek s (hi s))

-- dump implementation for sorted lists with inefficient seek
fromSorted :: Ord k => [(k,v)] -> Maybe (Seek k v)
fromSorted [] = Nothing
fromSorted l@((k,v):kvs) = Just (Seek k k v (fromSorted kvs) seeker)
  where seeker key = fromSorted $ dropWhile ((< key) . fst) l

fromList :: Ord k => [(k,v)] -> Maybe (Seek k v)
fromList = fromSorted . sortBy (comparing fst)

traceFromList :: (Show k, Ord k) => String -> [(k,v)] -> Maybe (Seek k v)
traceFromList name kvs =
  fromSorted [(trace (name ++ " " ++ show k) k, v)
             | (k,v) <- sortBy (comparing fst) kvs]

-- Merging two sorted iterators.
instance Functor (Seek k) where
  fmap :: (v -> u) -> Seek k v -> Seek k u
  fmap f s = s { value = f (value s)
               , bump = fmap (fmap f) (bump s)
               , seek = \k -> fmap (fmap f) (seek s k)
               }

merge :: Ord k => Seek k v1 -> Seek k v2 -> Seek k (v1,v2)
merge s t = Seek { lo = if leftLo then lo s else lo t
                 , hi = max (hi s) (hi t)
                 , value = (value s, value t)
                 -- TODO: wait, isn't this wrong? we want to bump the HIGH thing!!
                 -- THIS IS WROOOONG. maybe. but it's giving the right result. hmmmmmm.
                 , bump = if leftLo
                          then (`merge` t) <$> bump s
                          else (s `merge`) <$> bump t
                 , seek = if leftLo
                          then seeker merge s t
                          else seeker (flip merge) t s
                 }
  where
    leftLo = lo s <= lo t
    seeker merge s t k = do
      s' <- seek s k
      t' <- assert (k <= lo s' && lo s' <= hi s') $
            seek t (hi s')
      return $ assert (lo s' <= hi s' &&
                       hi s' <= lo t' &&
                       lo t' <= hi t')
             $ merge s' t'


-- Examples
list1 = [(1, "one"), (2, "two"), (3, "three")]
list2 = [(1, "a"), (3, "c"), (5, "e")]

ms1 = fromList list1
ms2 = fromList list2

m = merge <$> ms1 <*> ms2

xs = traceFromList "x" [(x,x) | x <- [1,3 .. 100]]
ys = traceFromList "y" [(y,y) | y <- [2,4 .. 100]]
zs = traceFromList "z" [(z,z) | z <- [1,100]]

mxyz = merge <$> (merge <$> xs <*> ys) <*> zs
