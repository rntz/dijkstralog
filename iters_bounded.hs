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

-- Sorted, seekable iterators as a coinductive type.
data Seek k v = Seek
  { lo :: !k
  , hi :: !k
  -- `value` and `next` should only be accessed when lo == hi.
  , value :: v
  , next :: Maybe (Seek k v)
  , seek :: k -> Maybe (Seek k v)
  }

deriving instance Functor (Seek k)

-- Intersects two seekable iterators, pairing their values when the keys match.
pair :: Ord k => Seek k v1 -> Seek k v2 -> Seek k (v1,v2)
pair s t = Seek { lo = min (lo s) (lo t)
                , hi = max (hi s) (hi t)
                , value = (value s, value t)
                , next = pair <$> next s <*> next t
                , seek = \k -> do
                           s' <- seek s k
                           t' <- seek t (hi s')
                           return $ pair s' t'
                }

map2 :: Ord k => (v -> u -> w) -> Seek k v -> Seek k u -> Seek k w
map2 f s t = uncurry f <$> pair s t

list :: Ord k => [Seek k v] -> Seek k [v]
list ts = self
  where self = Seek { lo = minimum (lo <$> ts)
                    , hi = maximum (hi <$> ts)
                    , value = value <$> ts
                    , next = list <$> traverse next ts
                    , seek = \k -> list <$> evalStateT (mapM seekOne ts) k
                    }
        seekOne t = do k <- get
                       t' <- lift $ seek t k
                       put (hi t')
                       return t'

instance (Ord k, Semigroup v) => Semigroup (Seek k v) where
  (<>) = map2 (<>)


-- Converting into & out of lists.
drain :: Eq k => Maybe (Seek k v) -> [(k,v)]
drain Nothing = []
drain (Just s)
  | lo s == hi s = (lo s, value s) : drain (next s)
  | otherwise    = drain (seek s (hi s))

-- dump implementation for sorted lists with inefficient seek
fromSorted :: Ord k => [(k,v)] -> Maybe (Seek k v)
fromSorted [] = Nothing
fromSorted l@((k,v):kvs) = Just (Seek k k v (fromSorted kvs) seeker)
  where seeker key = fromSorted $ dropWhile ((< key) . fst) l

fromList :: Ord k => [(k,v)] -> Maybe (Seek k v)
fromList = fromSorted . sortBy (comparing fst)

traceFromSorted name [] = Nothing
traceFromSorted name l@((k,v):kvs) = Just (Seek kt kt v (traceFromSorted name kvs) seeker)
  where kt = trace (name ++ " " ++ show k) k
        seeker key = traceFromSorted name $ dropWhile ((< key) . fst) l

traceFromList :: (Show k, Ord k) => String -> [(k,v)] -> Maybe (Seek k v)
traceFromList name kvs = traceFromSorted name $ sortBy (comparing fst) kvs

instance (Eq k, Show k, Show v) => Show (Seek k v) where
  show s = "{lo = " ++ show (lo s)
           ++ ", hi = " ++ show (hi s)
           ++ (if lo s == hi s then ", value = " ++ show (value s) else "")
           ++ "}"


-- Examples
list1 = [(1, "one"), (2, "two"), (3, "three"), (5, "five")]
list2 = [(1, "a"), (3, "c"), (5, "e")]

ms1 = fromList list1
ms2 = fromList list2

m = pair <$> ms1 <*> ms2

xs = traceFromList "x" [(x,x) | x <- [1,3 .. 100]]
ys = traceFromList "y" [(y,y) | y <- [2,4 .. 100]]
zs = traceFromList "z" [(z,z) | z <- [1,100]]

mxyz = pair <$> (pair <$> xs <*> ys) <*> zs

-- avoids interleaving of `trace` output with printing of value at REPL
draino x = length xs `seq` xs
  where xs = drain x
