{-# LANGUAGE StrictData #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FunctionalDependencies #-}
import Data.Array
import Text.Printf
import System.CPUTime
import System.IO (hFlush, stdout)


---------- SEARCH ----------
-- galloping search: exponential probing followed by binary search.
-- O(log i) where i is the returned index.
gallop :: (Int -> Bool) -> Int -> Int -> Int
gallop p lo hi = if p lo then lo else go lo 1
  where go lo step
          | lo + step >= hi = search p lo hi
          | p (lo + step)   = search p lo (lo + step)
          | otherwise       = go (lo + step) (step * 2)

-- binary search, modified from
-- https://byorgey.wordpress.com/2023/01/01/competitive-programming-in-haskell-better-binary-search/
search :: (Int -> Bool) -> Int -> Int -> Int
search p l r | r - l <= 1 = r
             | p m        = search p l m
             | otherwise  = search p m r
  where m = (l+r) `div` 2


---------- SEEKABLE ITERATORS ----------
data Bound k = Atleast k | Greater k deriving (Eq, Show)

instance Ord k => Ord (Bound k) where
  compare x y = embed x `compare` embed y
    where embed (Atleast k) = (k, 0)
          embed (Greater k) = (k, 1)

satisfies :: Ord k => Bound k -> k -> Bool
satisfies bound k = bound <= Atleast k

class Ord (Key it) => Seekable it where
  type Key it
  type Value it
  -- seeks forward the first thing satisfying the bound.
  seek :: it -> (Bound (Key it)) -> Seek it

data Seek it = Empty | Yield (Key it) (Maybe (Value it)) it

toSorted :: Seekable it => Seek it -> [(Key it, Value it)]
toSorted Empty = []
toSorted (Yield k (Just v) iter) = (k, v) : toSorted (seek iter (Greater k))
toSorted (Yield k Nothing iter) = toSorted $ seek iter (Atleast k)

intersect :: (Seekable s, Seekable t, Key s ~ Key t) => Seek s -> Seek t -> Seek (s, t)
intersect Empty _ = Empty
intersect _ Empty = Empty
intersect (Yield k x s) (Yield k' y t)
  | k == k'   = Yield k ((,) <$> x <*> y) (s, t)
  | otherwise = Yield (max k k') Nothing  (s, t)

instance (Seekable s, Seekable t, Key s ~ Key t) => Seekable (s, t) where
  type Key   (s, t) = Key s
  type Value (s, t) = (Value s, Value t)
  seek (s, t) p = intersect s' t' where
    s' = seek s p
    t' = seek t (let Yield k _ _ = s' in Atleast k)

data SA k v = SA { array :: !(Array Int (k, v))
                 , index :: !Int } deriving Show

seekSortedArray :: Ord k => Array Int (k, v) -> Int -> Seek (SA k v)
seekSortedArray arr idx | idx < hi  = Yield k (Just v) (SA arr idx)
                        | otherwise = Empty
  where (_, hi) = bounds arr
        (k, v)  = arr ! idx

fromSortedArray :: Ord k => [(k, v)] -> Seek (SA k v)
fromSortedArray l = seekSortedArray (listArray (0, length l) l) 0

instance Ord k => Seekable (SA k v) where
  type Key   (SA k v) = k
  type Value (SA k v) = v
  seek (SA arr i) p   = seekSortedArray arr $ gallop (satisfies p . fst . (arr !)) i hi
    where (_, hi) = bounds arr


---------- BENCHMARKING ----------
n = 50_000_000
evens = fromSortedArray [(x, "even") | x <- [0, 2 .. n]]
odds  = fromSortedArray [(x, "odd")  | x <- [1, 3 .. n]]
ends  = fromSortedArray [(x, "end")  | x <- [0,      n]]

printTime :: String -> a -> IO ()
printTime label result = do
  putStr (label ++ ": "); hFlush stdout
  start_ps <- getCPUTime --in picoseconds
  end_ps <- result `seq` getCPUTime
  printf "%0.6fs\n" (fromIntegral (end_ps - start_ps) / (10^12))

{-# NOINLINE time2 #-}
time2 label xs ys = printTime label $ length $ toSorted $ xs `intersect` ys
{-# NOINLINE time3L #-}
time3L label xs ys zs = printTime label $ length $ toSorted $ (xs `intersect` ys) `intersect` zs
{-# NOINLINE time3R #-}
time3R label xs ys zs = printTime label $ length $ toSorted $ xs `intersect` (ys `intersect` zs)

thrice x = do x; x; x
main = do thrice $ time2 "odds & ends" odds ends
          thrice $ time2 "evens & odds" evens odds
          thrice $ time3L "(evens & odds) & ends" evens odds ends
          thrice $ time3R "evens & (odds & ends)" evens odds ends
