import Data.Array; import Text.Printf; import System.CPUTime; import System.IO (hFlush, stdout)
data Iter k v = Empty | Yield k v (k -> Iter k v)

toSorted Empty = []
toSorted (Yield k v s) = (k,v) : toSorted (s k)

intersect Empty _ = Empty
intersect _ Empty = Empty
intersect s@(Yield k1 x s') t@(Yield k2 y t') =
  case compare k1 k2 of
    LT -> intersect (s' k2) t -- s < t, so seek s toward t
    GT -> intersect s (t' k1) -- t < s, so seek t toward s
    EQ -> Yield k1 (x,y) (\k' -> s' k' `intersect` t' k')

n = 30_000_000
evens = fromSortedArray [(x, "even") | x <- [0, 2 .. n]]
odds  = fromSortedArray [(x, "odd")  | x <- [1, 3 .. n]]
ends  = fromSortedArray [(x, "end")  | x <- [0,      n]]

fromSortedArray :: Ord k => [(k,v)] -> Iter k v
fromSortedArray l = go 0 where
  arr = listArray (0, hi) l
  hi = length l
  go lo = if lo >= hi then Empty else Yield k v seek where
    (k, v) = arr ! lo
    seek tgt = go $ gallop ((tgt <=) . fst . (arr !)) (lo + 1) hi

-- galloping search: exponential probing followed by binary search.
-- O(log i) where i is the returned index.
gallop :: (Int -> Bool) -> Int -> Int -> Int
gallop p lo hi | lo >= hi  = hi
               | p lo      = lo
               | otherwise = go lo 1 where
  go lo step | lo + step >= hi = bisect lo hi
             | p (lo + step)   = bisect lo (lo + step)
             | otherwise       = go (lo + step) (step * 2)
  bisect l r | r - l <= 1 = r
             | p mid      = bisect l mid
             | otherwise  = bisect mid r
    where mid = (l+r) `div` 2

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
