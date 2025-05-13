-- BUG: drain m produces infinite list!

-- Maybe I should collab with Joachim Breitner on this?
-- Or, who did the generalized radix sort work? Fritz Henglein.

import Prelude hiding (head, tail, sum, product)

import Control.Exception (assert)
import Data.Foldable (Foldable (toList, foldMap))
import Data.List (sortBy, insertBy)
import Data.List.NonEmpty (NonEmpty (..), head, tail, uncons, nonEmpty)
import Data.Maybe (mapMaybe, catMaybes)
import Data.Ord (comparing)
import Data.Semigroup (sconcat)
import qualified Data.List.NonEmpty as NE

import Control.Monad (guard)
import Control.Monad.State

import Debug.Trace (trace)

-- Applicative without pure. A "semimonoidal functor".
class Functor f => Apply f where
  map2 :: (a -> b -> c) -> f a -> f b -> f c
  pair :: f a -> f b -> f (a,b)
  pair = map2 (,)
  sequence1 :: NonEmpty (f a) -> f (NonEmpty a)
  sequence1 (x :| []) = (:|[]) <$> x
  sequence1 (x :| (y:ys)) = map2 (\x (y :| ys) -> x :| (y:ys)) x (sequence1 (y :| ys))

-- Seekable iterators as a coinductive type.
data Frog k v = Frog { posn :: k
                     , value :: Maybe v
                     , next :: k -> Maybe (Frog k v) }
                deriving Functor

seek :: Ord k => k -> Frog k v -> Maybe (Frog k v)
seek targ frog | posn frog >= targ = Just frog
               | otherwise         = next frog targ

instance Ord k => Apply (Frog k) where
  map2 f s t = Frog { posn = posn s `max` posn t
                    , value = do guard $ posn s == posn t
                                 f <$> value s <*> value t
                    , next = \targ -> do
                               s' <- seek targ s
                               t' <- assert (targ <= posn s') $
                                     seek (posn s') t
                               return $ map2 f s' t'
                    }


-- Wrapping (Maybe (Frog k v)) into a newtype to give it useful instances.
newtype Iter k v = Iter { frog :: Maybe (Frog k v) } deriving Functor
instance Ord k => Apply (Iter k) where
  map2 f (Iter s) (Iter t) = Iter $ map2 f <$> s <*> t

filter :: (k -> v -> Bool) -> Iter k v -> Iter k v
filter test = filterMap (\k v -> if test k v then Just v else Nothing)

filterMap :: (k -> v1 -> Maybe v2) -> Iter k v1 -> Iter k v2
filterMap func (Iter f) = Iter (loop <$> f)
  where loop f = Frog { posn = posn f
                      , value = func (posn f) =<< value f
                      , next = \k -> loop <$> next f k }


-- Union-merge.
instance (Ord k, Semigroup v) => Semigroup (Iter k v) where sconcat (x :| xs) = mconcat (x:xs)
instance (Ord k, Semigroup v) => Monoid (Iter k v) where
  mempty = Iter Nothing
  mconcat iters = union (mapMaybe frog iters)

union :: Ord k => [Frog k v] -> Iter k v
union frogs = unionSorted $ sortBy (comparing posn) frogs

unionSorted = undefined --TODO


-- Converting into and out of lists
drain :: Iter k v -> [(k,v)]
drain (Iter x) = loop x
  where loop Nothing = []
        loop (Just f) | Just v <- value f = (posn f, v) : rest
                      | otherwise         = rest
          where rest = loop (next f (posn f))

fromSorted :: Ord k => [(k,v)] -> Iter k v
fromSorted xs = Iter $ loop xs
  where loop xs = do
          ((k,v) :| rest) <- nonEmpty xs
          return $ Frog k (Just v) $ \targ -> loop $ dropWhile ((< targ) . fst) rest

fromList :: Ord k => [(k,v)] -> Iter k v
fromList = fromSorted . sortBy (comparing fst)

traceFromList :: (Show k, Ord k) => String -> [(k,v)] -> Iter k v
traceFromList name xs =
  fromSorted [(trace (name ++ " " ++ show k) k, v)
             | (k,v) <- sortBy (comparing fst) xs]



-- pair :: Ord k => Seek k v1 -> Seek k v2 -> Seek k (v1,v2)
-- pair s t = Seek
--            { key = key s `max` key t
--            , value = do guard (key s == key t)
--                         (v, s') <- value s
--                         (u, t') <- value t
--                         return ((v,u), liftA2 pair s' t')
--            , seek = \target -> do
--                       s' <- seek s target
--                       t' <- seek t (key s')
--                       return $ pair s' t'
--            }

-- map2 :: Ord k => (v -> u -> w) -> Seek k v -> Seek k u -> Seek k w
-- map2 f s t = uncurry f <$> pair s t

-- list :: Ord k => [Seek k v] -> Seek k [v]
-- list ts = self
--   where self = Seek { key = maximum (key <$> ts)
--                     , value = do guard $ and [key t == key self | t <- ts]
--                                  (vs, nexts) <- unzip <$> traverse value ts
--                                  return (vs, list <$> sequence nexts)
--                     , seek = \k -> list <$> evalStateT (mapM seekOne ts) k
--                     }
--         seekOne t = do k <- get
--                        t' <- lift $ seek t k
--                        put (key t')
--                        return t'

-- -- Can we _not_ call maximum every time? Let's see:
-- list' :: Ord k => [Seek k v] -> Seek k [v]
-- list' ts = loop (minimum (key <$> ts)) (maximum (key <$> ts)) ts
--   where loop lo hi ts =
--           assert (hi == maximum (map key ts)) $
--           assert (lo == minimum (map key ts)) $
--           Seek { key = hi
--                , value = do guard (lo == hi)
--                             (vs, nexts) <- unzip <$> traverse value ts
--                             return (vs, list <$> sequence nexts)
--                , seek = undefined
--                }

-- instance (Ord k, Semigroup v) => Semigroup (Seek k v) where
--   (<>) = map2 (<>)

-- 
-- -- Converting into & out of lists.
-- drain :: Eq k => Maybe (Seek k v) -> [(k,v)]
-- drain Nothing = []
-- drain (Just s)
--   | Just (v, rest) <- value s = (key s, v) : drain rest
--   | otherwise                 = drain (seek s (key s))

-- fromSorted :: Ord k => [(k,v)] -> Maybe (Seek k v)
-- fromSorted [] = Nothing
-- fromSorted l@((k,v):kvs) = Just $ Seek
--                            { key = k
--                            , value = Just (v, fromSorted kvs)
--                            -- it's important that we be able to revisit a value
--                            -- that we've already found!
--                            , seek = \targ -> fromSorted $ dropWhile ((< targ) . fst) l
--                            }

-- traceFromSorted :: (Show k, Ord k) => String -> [(k,v)] -> Maybe (Seek k v)
-- traceFromSorted name lst = loop lst
--   where loop [] = Nothing
--         loop l@((k,v):rest) = Just $ Seek
--                               { key = trace (name ++ " " ++ show k) k
--                               , value = Just (v, loop rest)
--                               , seek = \targ -> loop $ dropWhile ((< targ) . fst) l
--                               }

-- fromList :: Ord k => [(k,v)] -> Maybe (Seek k v)
-- fromList = fromSorted . sortBy (comparing fst)

-- traceFromList :: (Show k, Ord k) => String -> [(k,v)] -> Maybe (Seek k v)
-- traceFromList name kvs = traceFromSorted name $ sortBy (comparing fst) kvs


-- Examples
list1 = [(1, "one"), (2, "two"), (3, "three")]
list2 = [(1, "a"), (3, "c"), (5, "e")]

ms1 = fromList list1
ms2 = fromList list2

m = pair ms1 ms2

xs = traceFromList "x" [(x,x) | x <- [1,3 .. 100]]
ys = traceFromList "y" [(y,y) | y <- [2,4 .. 100]]
zs = traceFromList "z" [(z,z) | z <- [1,100]]

mxyz = pair (pair xs ys) zs

-- avoids interleaving of `trace` output with printing of value at REPL
draino x = length xs `seq` xs
  where xs = drain x
