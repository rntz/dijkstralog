{-# LANGUAGE TypeFamilies, FunctionalDependencies, ScopedTypeVariables #-}

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
                     -- [(lo, advanceLo), ...]
                     , nexts :: [(k, k -> Maybe (Frog k v))]
                     }
                deriving Functor

seekSorted :: Ord k => [(k,v)] -> k -> [(k, Frog k v)]
seekSorted xs k = frogs $ dropWhile ((< k) . fst) xs
  where frogs [] = []
        -- THIS IS INCORRECT.
        frogs ((k,v):rest) = [Frog k (Just v) (seekSorted rest)]

fromSorted :: Ord k => [(k,v)] -> Maybe (Frog k v)
fromSorted [] = Nothing
fromSorted ((k,v):rest) = Just $ Frog k (Just v) (seekSorted rest)

-- seek :: Ord k => k -> Frog k v -> Maybe (Frog k v)
-- seek targ frog | posn frog >= targ = Just frog
--                | otherwise         = next frog targ

-- instance Ord k => Apply (Frog k) where
--   map2 f s t = Frog { posn = posn s `max` posn t
--                     , value = do guard $ posn s == posn t
--                                  f <$> value s <*> value t
--                     , next = \targ -> do
--                                s' <- seek targ s
--                                t' <- assert (targ <= posn s') $
--                                      seek (posn s') t
--                                return $ map2 f s' t'
--                     }

-- 
-- -- Wrapping (Maybe (Frog k v)) into a newtype to give it useful instances.
-- newtype Iter k v = Iter { frog :: Maybe (Frog k v) } deriving Functor
-- instance Ord k => Apply (Iter k) where
--   map2 f (Iter s) (Iter t) = Iter $ map2 f <$> s <*> t

-- drain :: Iter k v -> [(k,v)]
-- drain (Iter x) = loop x
--   where loop Nothing = []
--         loop (Just f) | Just v <- value f = (posn f, v) : rest
--                       | otherwise         = rest
--           where rest = loop (next f (posn f))

-- fromSorted :: Ord k => [(k,v)] -> Iter k v
-- fromSorted xs = Iter $ loop xs
--   where loop xs = do
--           ((k,v) :| rest) <- nonEmpty xs
--           return $ Frog k (Just v) $ \targ -> loop $ dropWhile ((< targ) . fst) rest
