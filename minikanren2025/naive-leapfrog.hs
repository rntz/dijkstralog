import Data.Array (Array)
import Data.Array.IArray

import Text.Printf
import System.CPUTime
import System.IO (hFlush, stdout)

time :: IO t -> IO (Double, t)
time k = do
  start_ps <- getCPUTime --in picoseconds
  result <- k
  end_ps <- getCPUTime
  let diff_s = fromIntegral (end_ps - start_ps) / (10^12)
  return (diff_s, result)

printTime :: IO t -> IO t
printTime k = do (time, result) <- time k
                 printf "Computation time: %0.3fs\n" time
                 return result

data Iter k v = Empty
              | Yield k v (k -> Iter k v)

-- fromSorted :: Ord k => [(k, v)] -> Iter k v
-- fromSorted [] = Empty
-- fromSorted ((k,v) : rest) = Yield k v seek
--   where seek k' = fromSorted (dropWhile ((< k') . fst) rest)

toSorted :: Iter k v -> [(k, v)]
toSorted Empty = []
toSorted (Yield k v s) = (k,v) : toSorted (s k)

intersect :: Ord k => Iter k a -> Iter k b -> Iter k (a,b)
intersect Empty _ = Empty
intersect _ Empty = Empty
intersect s@(Yield k1 x s') t@(Yield k2 y t') =
  case compare k1 k2 of
    LT -> intersect (s' k2) t -- s < t, so seek s toward t
    GT -> intersect s (t' k1) -- t < s, so seek t toward s
    EQ -> Yield k1 (x,y) (\k' -> s' k' `intersect` t' k')

xs = fromSorted [(1, "one"), (2, "two"), (3, "three")]
ys = fromSorted [(1, "wun"), (3, "tres")]
xys = xs `intersect` ys

n = 30_000_000
evens = fromSorted [(x, "even") | x <- [0, 2 .. n]]
odds  = fromSorted [(x, "odd")  | x <- [1, 3 .. n]]
ends  = fromSorted [(x, "end")  | x <- [0,      n]]

{-# NOINLINE swizzle #-}
swizzle :: Ord k => Iter k a -> Iter k b -> IO ()
swizzle xs ys = printTime $ print $ length $ toSorted $ xs `intersect` ys
{-# NOINLINE swizzle2 #-}
swizzle2 xs ys zs = printTime $ print $ length $ toSorted $ (xs `intersect` ys) `intersect` zs
{-# NOINLINE swizzle3 #-}
swizzle3 xs ys zs = printTime $ print $ length $ toSorted $ xs `intersect` (ys `intersect` zs)

main = do
  putStr "odds ∩ ends... "; hFlush stdout
  swizzle odds ends
  putStr "odds ∩ ends... "; hFlush stdout
  swizzle odds ends
  putStr "odds ∩ ends... "; hFlush stdout
  swizzle odds ends

  putStr "evens ∩ odds... "; hFlush stdout
  swizzle evens odds
  putStr "evens ∩ odds... "; hFlush stdout
  swizzle evens odds
  putStr "evens ∩ odds... "; hFlush stdout
  swizzle evens odds

  putStr "evens ∩ (odds ∩ ends)... "; hFlush stdout
  swizzle3 evens odds ends
  putStr "evens ∩ (odds ∩ ends)... "; hFlush stdout
  swizzle3 evens odds ends
  putStr "evens ∩ (odds ∩ ends)... "; hFlush stdout
  swizzle3 evens odds ends

  putStr "(evens ∩ odds) ∩ ends... "; hFlush stdout
  swizzle2 evens odds ends
  putStr "(evens ∩ odds) ∩ ends... "; hFlush stdout
  swizzle2 evens odds ends
  putStr "(evens ∩ odds) ∩ ends... "; hFlush stdout
  swizzle2 evens odds ends

fromSorted :: Ord k => [(k,v)] -> Iter k v
fromSorted l = arr `seq` fromArray arr bounds
  where bounds = (0, length l)
        arr = listArray bounds l

fromArray :: Ord k => Array Int (k, v) -> (Int, Int) -> Iter k v
fromArray arr (lo, hi) = if lo >= hi then Empty else Yield k v seek where
  (k,v) = arr ! lo
  seek tgt | lo + 1 >= hi = Empty
           | otherwise = fromArray arr (lo', hi)
    where lo' = gallop ((tgt <=) . fst . (arr !)) (lo + 1) hi

-- fromIArray :: (IArray a k, Ord k) => a Int k -> Iter k ()
-- fromIArray arr = go lo where
--   (lo, hi) = bounds arr
--   go i = Seek pos sek where
--     pos | i >= hi   = Bound Done
--         | otherwise = Found k () where k = arr ! i
--     sek tgt = go $ gallop (satisfies tgt . (arr !)) i hi

-- galloping search
gallop :: (Int -> Bool) -> Int -> Int -> Int
gallop p lo hi = if p lo then lo else go lo 1
  where go lo step
          | lo + step >= hi = snd $ search mid p lo hi
          | p (lo + step) = snd $ search mid p lo (lo + step)
          | otherwise = go (lo + step) (step * 2)

-- binary search
-- https://byorgey.wordpress.com/2023/01/01/competitive-programming-in-haskell-better-binary-search/
search :: (a -> a -> Maybe a) -> (a -> Bool) -> a -> a -> (a, a)
search mid p = go where
  go l r = case mid l r of Nothing -> (l,r)
                           Just m | p m       -> go l m
                                  | otherwise -> go m r

mid l r | r - l > 1 = Just ((l+r) `div` 2)
        | otherwise = Nothing
