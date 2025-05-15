{-# LANGUAGE TypeFamilies, FunctionalDependencies, ScopedTypeVariables #-}

import Prelude hiding (head, tail, sum, product, mapMaybe)

import Control.Exception (assert)
import Data.Foldable (Foldable (toList, foldMap))
import Data.List (sortBy, insertBy)
import Data.List.NonEmpty (NonEmpty (..), head, tail, uncons, nonEmpty)
import Data.Ord (comparing)
import qualified Data.List.NonEmpty as NE
import Data.Coerce (coerce)

import Debug.Trace (trace)

-- Applicative without pure. A "semimonoidal functor".
class Functor f => Apply f where
  map2 :: (a -> b -> c) -> f a -> f b -> f c
  pair :: f a -> f b -> f (a,b)
  pair = map2 (,)

data Bound = AtLeast | Greater deriving (Show, Eq, Ord)
matches :: Ord k => Bound -> k -> k -> Bool
matches AtLeast k k' = k <= k'
matches Greater k k' = k  < k'

newtype Iter k v = Iter { run :: Maybe (Seek k v) } deriving Functor --possibly empty

-- Sorted, seekable iterators as a coinductive type.
data Seek k v = Seek --nonempty
  { key :: !k
  , value :: Maybe v
  -- (seek b tgt) seeks toward the first k such that (matches b tgt k).
  -- ie. let b = AtLeast to find first key >= tgt
  --     let b = Greater to find first key  > tgt
  , seek :: Bound -> k -> Iter k v
  -- PRECONDITION:      key s <= tgt
  -- POSTCONDITION:     matches b tgt (key (seek s b tgt))
  -- idempotent: if below targ incl (key s) then seek s targ incl == Just s
  }
deriving instance Functor (Seek k)

onIter :: (Seek k v -> Seek k u) -> Iter k v -> Iter k u
onIter f = Iter . fmap f . run

onSeek :: (Iter k v -> Iter k u) -> Maybe (Seek k v) -> Maybe (Seek k u)
onSeek f = run . f . Iter

instance Ord k => Apply (Seek k) where
  map2 :: Ord k => (a -> b -> c) -> Seek k a -> Seek k b -> Seek k c
  map2 f s t = Seek rk rv rs
    where rk = key s `max` key t
          rv = if key s == key t then f <$> value s <*> value t else Nothing
          rs b k = Iter $ do s' <- run $ seek s b k
                             t' <- run $ seek t AtLeast (key s')
                             return $ map2 f s' t'


-- Possibly-empty seekable iterators.
instance Ord k => Apply (Iter k) where
  map2 f s t = Iter (map2 f <$> run s <*> run t)

class Functor f => MapMaybe f where
  mapMaybe :: (a -> Maybe b) -> f a -> f b
  catMaybes :: f (Maybe a) -> f a
  catMaybes = mapMaybe id

instance MapMaybe (Seek k) where
  mapMaybe f t@(Seek k v _) = Seek k (v >>= f) (\b k -> mapMaybe f $ seek t b k)

instance MapMaybe (Iter k) where
  mapMaybe f = onIter (mapMaybe f)

filterMap :: (k -> v -> Maybe u) -> Iter k v -> Iter k u
filterMap f = onIter loop
  where loop t@(Seek k v _) = Seek k (v >>= f k) (\b k -> filterMap f $ seek t b k)

mapWithKey :: (k -> v -> u) -> Iter k v -> Iter k u
mapWithKey f = filterMap (\k v -> Just (f k v))

data LR x y = L x | R y | B x y

left :: LR x y -> Maybe x
left (B x y) = Just x
left (L x) = Just x
left (R _) = Nothing

rght :: LR x y -> Maybe y
rght (B x y) = Just y
rght (R y) = Just y
rght (L _) = Nothing

lr :: Maybe x -> Maybe y -> Maybe (LR x y)
lr (Just x) (Just y) = Just $ B x y
lr (Just x) Nothing = Just $ L x
lr Nothing (Just y) = Just $ R y
lr Nothing Nothing = Nothing

-- class Apply f => OuterJoin f where
--   outerJoin :: (LR a b -> c) -> f a -> f b -> f c

outerJoin :: forall k v u w. Ord k => (k -> LR v u -> Maybe w) -> Iter k v -> Iter k u -> Iter k w
outerJoin f s t = Iter $ do st <- lr (run s) (run t)
                            case st of
                              L xs -> filterMap (\k v -> f k (L v)) `onSeek` Just xs
                              R ys -> filterMap (\k v -> f k (R v)) `onSeek` Just ys
                              B xs ys -> undefined

-- outerJoin f = coerce oj
--   where onlyL = undefined
--         oj :: Maybe (Seek k v) -> Maybe (Seek k u) -> Maybe (Seek k w)
--         oj Nothing t = contents . filterMap (\k u -> f k Nothing (Just u)) $ Iter t
--         oj s Nothing = contents . filterMap (\k v -> f k (Just v) Nothing) $ Iter s
--         oj (Just s) (Just t) = Just $ Seek undefined undefined undefined

-- outerJoin f (Iter Nothing) t = filterMap (\k u -> f k Nothing (Just u)) t
-- outerJoin f s (Iter Nothing) = filterMap (\k v -> f k (Just v) Nothing) s
-- outerJoin f (Iter (Just s@(Seek sk sv ss))) (Iter (Just t@(Seek tk tv ts))) =
--   Iter $ Just $ Seek
--   { key = min sk tk
--   , value = undefined
--   , seek = \b tgt -> coerce (outerJoin f) (ss b tgt) (ts b tgt)
--   }


-- Converting into & out of sorted lists.
drain :: Iter k v -> [(k,v)]
drain (Iter Nothing) = []
drain (Iter (Just s))
  | Just v <- value s = (key s, v) : drain (seek s Greater (key s))
  | otherwise = drain (seek s AtLeast (key s))

fromSorted :: Ord k => [(k,v)] -> Iter k v
fromSorted l = Iter $ do (k,v) :| _ <- nonEmpty l
                         return $ Seek k (Just v) s
  where s b tgt = fromSorted $ dropWhile (not . matches b tgt . fst) l

traceFromSorted name l = Iter $ do (k,v) :| _ <- nonEmpty l
                                   let kt = trace (name ++ " " ++ show k) k
                                   return $ Seek kt (Just v) s
  where s b tgt = traceFromSorted name $ dropWhile (not . matches b tgt . fst) l

fromList :: Ord k => [(k,v)] -> Iter k v
traceFromList :: (Show k, Ord k) => String -> [(k,v)] -> Iter k v
fromList = fromSorted . sortBy (comparing fst)
traceFromList name = traceFromSorted name . sortBy (comparing fst)

-- 
-- -- Examples
-- list1 = [(1, "one"), (2, "two"), (3, "three"), (5, "five")]
-- list2 = [(1, "a"), (3, "c"), (5, "e")]

-- ms1 = traceFromList "A" list1
-- ms2 = traceFromList "B" list2

-- m = pair <$> ms1 <*> ms2

-- xs = traceFromList "x" [(x,x) | x <- [1,3 .. 100]]
-- ys = traceFromList "y" [(y,y) | y <- [2,4 .. 100]]
-- zs = traceFromList "z" [(z,z) | z <- [1,100]]

-- mxyz = pair <$> (pair <$> xs <*> ys) <*> zs

-- -- avoids interleaving of `trace` output with printing of value at REPL
-- draino x = length xs `seq` xs
--   where xs = drain x

