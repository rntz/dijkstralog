{-# LANGUAGE TypeFamilies, FunctionalDependencies, ScopedTypeVariables, PartialTypeSignatures #-}

import Prelude hiding (head, tail, sum, product, mapMaybe)

import Control.Exception (assert)
import Control.Monad (guard)
import Data.Coerce (coerce)
import Data.Foldable (Foldable (toList, foldMap))
import Data.List (sortBy, insertBy)
import Data.List.NonEmpty (NonEmpty (..), head, tail, uncons, nonEmpty)
import Data.Ord (comparing)
import qualified Data.List.NonEmpty as NE

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

-- Sorted, seekable iterators as a coinductive type.
-- Iter k v = we know whether it's empty or not.
-- Seek k v = we know a lower bound on the remaining keys.
newtype Iter k v = Iter { run :: Maybe (Seek k v) } deriving Functor
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

-- onSeek :: (Seek k v -> Seek k u) -> Iter k v -> Iter k u
-- onSeek f = Iter . fmap f . run

-- onIter :: (Iter k v -> Iter k u) -> Maybe (Seek k v) -> Maybe (Seek k u)
-- onIter f = run . f . Iter

instance Ord k => Apply (Seek k) where
  map2 :: Ord k => (a -> b -> c) -> Seek k a -> Seek k b -> Seek k c
  map2 f s t = Seek rk rv rs
    where rk = key s `max` key t
          rv = if key s == key t then f <$> value s <*> value t else Nothing
          rs b k = Iter $ do s' <- run $ seek s b k
                             t' <- run $ seek t AtLeast (key s')
                             return $ map2 f s' t'

instance Ord k => Apply (Iter k) where
  map2 f s t = Iter (map2 f <$> run s <*> run t)


-- -- MapMaybe. Seems unnecessary.
-- class Functor f => MapMaybe f where
--   mapMaybe :: (a -> Maybe b) -> f a -> f b
--   catMaybes :: f (Maybe a) -> f a
--   catMaybes = mapMaybe id

-- instance MapMaybe (Seek k) where
--   mapMaybe f t@(Seek k v _) = Seek k (v >>= f) (\b k -> mapMaybe f $ seek t b k)

-- instance MapMaybe (Iter k) where
--   mapMaybe f = onSeek (mapMaybe f)


-- Outer joins, ie generalized unions.
-- Do we have a left thing, a right thing, or both?
data LR x y = L x | R y | B x y

lr :: (a -> c) -> (b -> c) -> (a -> b -> c) -> LR a b -> c
lr left rght both (L x) = left x
lr left rght both (R y) = rght y
lr left rght both (B x y) = both x y

class Functor f => OuterJoin f where
  outerPair :: f a -> f b -> f (LR a b)
  outerJoin :: (LR a b -> c) -> f a -> f b -> f c
  outerJoin f xs ys = f <$> outerPair xs ys

instance OuterJoin Maybe where
  -- -- Concise implementation:
  -- outerPair (Just x) y = Just $ maybe (L x) (B x) y
  -- outerPair Nothing  y = R <$> y

  -- A more explicit implementation is probably better pedagogically:
  outerPair (Just x) (Just y) = Just $ B x y
  outerPair (Just x) Nothing  = Just $ L x
  outerPair Nothing  (Just y) = Just $ R y
  outerPair Nothing  Nothing  = Nothing

instance Ord k => OuterJoin (Seek k) where
  outerPair s t =
    Seek { key = key s `min` key t
         , value = case compare (key s) (key t) of
                     LT -> L <$> value s
                     GT -> R <$> value t
                     EQ -> B <$> value s <*> value t
         , seek = \b k -> outerPair (seek s b k) (seek t b k)
         }

instance Ord k => OuterJoin (Iter k) where
  outerPair (Iter s) (Iter t) = Iter $ outerJoin combine s t
    where combine (B xs ys) = outerPair xs ys
          combine (L xs) = L <$> xs
          combine (R ys) = R <$> ys

-- Union via outer join: we combine the values using a commutative semigroup
-- operator. (If it's not commutative that's fine too, I guess? But make sure
-- you know what you're doing.)
unionWith :: Ord k => (v -> v -> v) -> Iter k v -> Iter k v -> Iter k v
unionWith f = outerJoin (lr id id f)

instance (Ord k, Semigroup v) => Semigroup (Iter k v) where
  (<>) = unionWith (<>)

instance (Ord k, Semigroup v) => Monoid (Iter k v) where
  mempty = Iter Nothing


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
draino x = length xs `seq` xs
  where xs = drain x

