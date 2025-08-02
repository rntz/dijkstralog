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
data Position k v = Found k v | Bound (Bound k) deriving Show
data Bound k = Atleast k | Greater k | Done deriving (Eq, Show)

instance Ord k => Ord (Bound k) where
  compare x y = embed x `compare` embed y
    where embed (Atleast k) = (1, Just (k, 0))
          embed (Greater k) = (1, Just (k, 1))
          embed Done        = (2, Nothing)

satisfies :: Ord k => Bound k -> k -> Bool
satisfies bound k = bound <= Atleast k

data Seek k v = Seek
  { posn :: Position k v          -- a key-value pair, or a bound
  , seek :: Bound k -> Seek k v } -- seeks forward toward a bound

toSorted :: Seek k v -> [(k,v)]
toSorted (Seek (Bound Done) _)   = []
toSorted (Seek (Bound p)   seek) = toSorted (seek p)
toSorted (Seek (Found k v) seek) = (k,v) : toSorted (seek (Greater k))

bound :: Seek k v -> Bound k
bound (Seek (Found k _) _) = Atleast k
bound (Seek (Bound p)   _) = p

intersect :: Ord k => Seek k a -> Seek k b -> Seek k (a,b)
intersect s t = Seek posn' seek' where
  posn' | Found k x <- posn s, Found k' y <- posn t, k == k' = Found k (x, y)
        | otherwise = Bound (bound s `max` bound t)
  seek' k = intersect s' t' where
    s' = seek s k
    t' = seek t (bound s') -- leapfrog optimization; could be (seek2 k) instead

empty :: Seek k v
empty = Seek (Bound Done) (const empty)

fromSortedArray :: Ord k => [(k,v)] -> Seek k v
fromSortedArray l = go 0 where
  hi = length l
  arr = listArray (0, hi) l
  go lo = if lo < hi then Seek (Found k v) seek else empty where
    (k, v) = arr ! lo
    seek tgt = go $ gallop (satisfies tgt . fst . (arr !)) lo hi


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
