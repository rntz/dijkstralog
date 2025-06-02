{-# LANGUAGE ScopedTypeVariables #-}
import Prelude hiding (fail)

-- Given a lower bound in key-space, spend a bounded amount of time trying to
-- compute a value of type a. We can improve the lower bound, and we also either
-- succeed and produce an a, or produce a `remainder` search process.
data Search k a = Search { run :: k -> (k, Either (Search k a) a) }
instance Functor (Search k) where
  fmap f (Search m) = Search $ \k -> undefined

fail :: Search k a
fail = Search (\k -> (k, Left fail))

succeed :: a -> Search k a
succeed x = Search (\k -> (k, Right x))

atLeast :: Ord k => k -> Search k a
atLeast k = Search (\k' -> (max k k', Left fail))

fromEither :: Either (Search k a) a -> Search k a
fromEither (Right x) = succeed x
fromEither (Left s) = s

-- This should be Zip not applicative! Maybe?
-- this runs the left/right halves "in parallel".
-- the Monad instance waits until the first finishes, then runs the right.
-- this means that the Monad instance DOES NOT ALLOW CROSSTALK between left/right halves!
instance Ord k => Applicative (Search k) where
  pure x = Search (\k -> (k, Right x))
  (<*>) :: forall k a b. Ord k => Search k (a -> b) -> Search k a -> Search k b
  sf <*> sx = Search sfx where
    sfx k = (k2, mfx) where
      (k1, mf) = run sf k
      (k2, mx) = run sx k1
      mfx = case (mf, mx) of
              (Right f, Right x) -> Right (f x)
              (Right f, Left x)  -> Left $ f <$> x
              (Left f, Right x)  -> Left $ ($ x) <$> f
              (Left f, Left x)   -> Left $ f <*> x

-- -- DANGER Will Robinson! this DOES NOT ALLOW CROSSTALK between a thing and its
-- -- continuation.
-- instance Ord k => Monad (Search k) where
--   s >>= kont = Search s' where
--     s' k = case run s k of
--              (k', Right x) -> run (kont x) k'
--              (k', Left  c) -> (k', Left (c >>= kont))

-- -- Iterators over key space.
-- -- NOPE THIS IS WRONG.
-- data Iter k a = Iter { next :: Search k (Maybe ((k, a), Iter k a)) }
--                 deriving Functor

-- class Functor f => Apply f where map2 :: (a -> b -> c) -> f a -> f b -> f c
-- instance Ord k => Apply (Iter k) where
--   map2 :: forall a b c. (a -> b -> c) -> Iter k a -> Iter k b -> Iter k c
--   map2 f s t = Iter $ combine <$> next s <*> next t
--     where combine :: Maybe ((k,a), Iter k a) -> Maybe ((k,b), Iter k b) -> Maybe ((k,c), Iter k c)
--           combine mx my = do ((k1,v1), rest1) <- mx
--                              ((k2,v2), rest2) <- my
--                              case k1 `compare` k2 of
--                                EQ -> return ((k1, f v1 v2), map2 f rest1 rest2)
--                                LT -> _wub
