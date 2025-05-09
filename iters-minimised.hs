{-# LANGUAGE FunctionalDependencies #-}
import Data.List.NonEmpty (NonEmpty (..), head, tail, uncons, nonEmpty)
import qualified Data.List.NonEmpty as NE
import Data.Foldable (Foldable (toList))
import Data.Semigroup

-- nonempty seekable iterators
class Ord k => Seek1 iter k v | iter -> k, iter -> v where
  key :: iter -> k
  value :: iter -> v
  next :: iter -> Maybe iter
  seek :: k -> iter -> Maybe iter

data Zip a = Zip { preRev :: [a], current :: a, post :: [a] } deriving Show
instance Foldable Zip where
  toList (Zip lsRev cur post) = reverse lsRev ++ cur : post
  foldMap f z = foldMap f (toList z)

rotate :: Zip a -> Zip a
rotate (Zip lsRev x (r:rs)) = Zip (x:lsRev) r rs
rotate (Zip lsRev x []) = Zip [] x' rs'
  where x' :| rs' = NE.reverse (x :| lsRev)

-- Invariant: in (Leapfrog its), all iterators in its have the same key.
data Leapfrog it k v = Leapfrog (Zip it) deriving Show

search :: (Monoid v, Seek1 it k v) => k -> Zip it -> Maybe (Leapfrog it k v)
search k z = loop k z count
  where
    count = length (preRev z) + length (post z)
    loop k z 0 = Just (Leapfrog z)
    loop k z n = do
      i <- seek k (current z)
      let k' = key i
      let z' = rotate (z { current = i })
      let n' = if k == k' then n-1 else count
      loop k' z' n'

-- We don't do the sort-by-min-key trick in the paper; our search doesn't need it.
leapfrogInit :: (Monoid v, Seek1 it k v) => NonEmpty (Maybe it) -> Maybe (Leapfrog it k v)
leapfrogInit xs = do
  it :| its <- sequence xs
  search (key it) (rotate (Zip [] it its))

instance (Monoid v, Seek1 iter k v) => Seek1 (Leapfrog iter k v) k v where
  key (Leapfrog z) = key (current z)
  value (Leapfrog z) = foldMap value z
  next (Leapfrog z) = do
    it <- next (current z)
    search (key it) (z { current = it })
  seek k l@(Leapfrog z)
    | k <= key l = Just l -- avoids N = (length z) comparisons
    | otherwise  = search k z
