import Data.Array

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

fromSorted :: Ord k => [(k,v)] -> Seek k v
fromSorted l = Seek posn seek where
  posn = case l of (k,v):_ -> Found k v
                   []      -> Bound Done
  seek target = fromSorted (dropWhile (not . satisfies target . fst) l)

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

n = 30_000_000
evens = fromSortedArray [(x, "even") | x <- [0, 2 .. n]]
odds  = fromSortedArray [(x, "odd")  | x <- [1, 3 .. n]]
ends  = fromSortedArray [(x, "end")  | x <- [0,      n]]

{-# NOINLINE swizzle #-}
swizzle :: Ord k => Seek k a -> Seek k b -> IO ()
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

-- --main = print $ length $ toSorted $ evens `intersect` odds `intersect` ends
-- main = do
--   print $ posn evens
--   print $ posn odds
--   print $ posn ends
--   putStr "evens ∩ odds... "; hFlush stdout
--   printTime $ print $ length $ toSorted $ evens `intersect` odds
--   putStr "evens ∩ odds... "; hFlush stdout
--   printTime $ print $ length $ toSorted $ evens `intersect` odds
--   putStr "(evens ∩ odds) ∩ ends... "; hFlush stdout
--   printTime $ print $ length $ toSorted $ (evens `intersect` odds) `intersect` ends
--   putStr "evens ∩ (odds ∩ ends)... "; hFlush stdout
--   printTime $ print $ length $ toSorted $ evens `intersect` (odds `intersect` ends)

fromSortedArray :: Ord k => [(k,v)] -> Seek k v
fromSortedArray l = go 0 where
  hi = length l
  arr = listArray (0, hi) l
  go lo = Seek posn seek where
    posn | lo >= hi  = Bound Done
         | otherwise = Found k v where (k,v) = arr ! lo
    seek tgt = go $ gallop (satisfies tgt . fst . (arr !)) lo hi

-- fromSorted :: Ord k => [(k,v)] -> Seek k v
-- fromSorted l = fromArray arr bounds
--   where bounds = (0, length l)
--         arr = listArray bounds l

-- fromArray :: Ord k => Array Int (k, v) -> (Int, Int) -> Seek k v
-- fromArray arr (lo, hi) = self where
--   self = Seek pos sek
--   pos | lo >= hi  = Bound Done
--       | otherwise = Found k v where (k,v) = arr ! lo
--   sek tgt = fromArray arr (lo', hi)
--     where lo' = gallop (satisfies tgt . fst . (arr !)) lo hi

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
