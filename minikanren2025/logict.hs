import Control.Monad
import Control.Applicative
import Data.Functor.Identity

class MonadPlus m => LogicM m where
  msplit :: m a -> m (Maybe (a, m a))

  interleave :: m a -> m a -> m a
  interleave sg1 sg2 = do
    r <- msplit sg1
    case r of Nothing -> sg2
              Just (sg11, sg12) -> return sg11 `mplus` interleave sg2 sg12

  (>>-) :: m a -> (a -> m b) -> m b
  sg >>- g = do
    r <- msplit sg
    case r of Nothing -> mzero
              Just (sg1, sg2) -> interleave (g sg1) (sg2 >>- g)

  ifte :: m a -> (a -> m b) -> m b -> m b
  ifte t th el = do
    r <- msplit t
    case r of
      Nothing -> el
      Just (sg1, sg2) -> th sg1 `mplus` (sg2 >>= th)

  once :: m a -> m a
  once m = do
    r <- msplit m
    case r of Nothing -> mzero
              Just (sg1, _) -> return sg1

reflect :: LogicM m => Maybe (a, m a) -> m a
reflect r = case r of
              Nothing -> mzero
              Just (a, tmr) -> return a `mplus` tmr


-- Implementations of LogicM
instance LogicM [] where
  msplit [] = return Nothing
  msplit (x:xs) = return $ Just (x, xs)

newtype SFKT m a =
  SFKT { unSFKT :: forall ans. SK (m ans) a -> FK (m ans) -> m ans }
type SK ans a = a -> FK ans -> ans
type FK ans = ans

deriving instance Functor m => Functor (SFKT m)

instance Monad m => Applicative (SFKT m) where
  pure e = SFKT (\sk fk -> sk e fk)
  liftA2 = liftM2

instance Monad m => Monad (SFKT m) where
  m >>= f = SFKT (\sk -> unSFKT m (\a -> unSFKT (f a) sk))

instance Monad m => Alternative (SFKT m) where
  empty = SFKT (\_ fk -> fk)
  m1 <|> m2 = SFKT (\sk fk -> unSFKT m1 sk (unSFKT m2 sk fk))

instance Monad m => MonadPlus (SFKT m)

-- this only works well for non-strict monads m
solve :: Monad m => SFKT m a -> m [a]
solve (SFKT m) = m (\a fk -> fk >>= (return . (a:))) (return [])

-- gets just the first answer
observe :: MonadFail m => SFKT m a -> m a
observe (SFKT m) = m (\a fk -> return a) (fail "no answer")

class MonadTrans t where lift :: Monad m => m a -> t m a
instance MonadTrans SFKT where
  lift m = SFKT (\sk fk -> m >>= (\a -> sk a fk))

instance Monad m => LogicM (SFKT m) where
  msplit tma = lift $ unSFKT tma ssk (return Nothing)
    where ssk a fk = return $ Just (a, lift fk >>= reflect)


-- Programs
fromList :: MonadPlus m => [a] -> m a
fromList [] = mzero
fromList (a:as) = return a `mplus` fromList as --no need for interleave here

odds, primes :: MonadPlus m => m Int
odds = fromList [1,3..]
primes = fromList (sieve [2..])
  where sieve (p : xs) = p : sieve [x | x <- xs, x `mod` p /= 0]

evenOdds, evenPrimes :: LogicM m => m Int
evenOdds = odds >>- (\x -> if even x then return x else mzero)
evenPrimes = primes >>- (\x -> if even x then return x else mzero)

incomplete1, incomplete2 :: LogicM m => m Int
incomplete1 = evenOdds `interleave` return 0
-- should generate 2, 3, and then hang - never producing 4.
incomplete2 = evenPrimes `interleave` fromList [3,4]
