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
data Bound k = Atleast k | Greater k | Done deriving (Eq, Show)
data Position k v = Found k v | Bound (Bound k) deriving (Eq, Show)

instance Ord k => Ord (Bound k) where
  compare = undefined --shouldn't get called
  -- compare x y = embed x `compare` embed y
  --   where embed (Atleast k) = (1, Just (k, 0))
  --         embed (Greater k) = (1, Just (k, 1))
  --         embed Done = (2, Nothing)
  max (Atleast x) (Atleast y) = Atleast (max x y)
  -- max (Greater x) (Greater y) = Greater (max x y)
  -- max (Atleast x) (Greater y) | x <= y    = Greater y
  --                             | otherwise = Atleast x
  -- max (Greater y) (Atleast x) | x <= y    = Greater y
  --                             | otherwise = Atleast x
  max Done        _           = Done
  max _           Done        = Done
  max _ _ = undefined --shouldn't get called

satisfies :: Ord k => Bound k -> k -> Bool
satisfies (Atleast x) y = x <= y
satisfies (Greater x) y = x <  y
satisfies Done        y = False

class Ord (Key it) => Seek it where
  type Key it
  type Value it
  posn :: it -> Position (Key it) (Value it)
  seek :: it -> (Bound (Key it)) -> it

toSorted :: Seek it => it -> [(Key it, Value it)]
toSorted it = case posn it of
                Bound Done -> []
                Bound p -> toSorted $ seek it p
                Found k v -> (k, v) : toSorted (seek it (Greater k))

bound :: Seek it => it -> Bound (Key it)
bound it = case posn it of Bound p -> p; Found k _ -> Atleast k

instance (Seek s, Seek t, Key s ~ Key t) => Seek (s, t) where
  type Key   (s, t) = Key s
  type Value (s, t) = (Value s, Value t)
  posn (s, t) | Found k x <- posn s, Found k' y <- posn t, k == k' = Found k (x, y)
              | otherwise = Bound (bound s `max` bound t)
  seek (s, t) p = (s', t') where
    s' = seek s p
    t' = seek t (bound s') -- leapfrog

intersect :: (Seek s, Seek t) => s -> t -> (s, t)
intersect s t = (s, t)

data SA k v = SA { array :: !(Array Int (k, v))
                 , index :: !Int }
              deriving Show

instance Ord k => Seek (SA k v) where
  type Key   (SA k v) = k
  type Value (SA k v) = v
  posn (SA arr idx) | idx < snd (bounds arr) = let (k, v) = arr ! idx in Found k v
                    | otherwise = Bound Done
  seek (SA arr idx) p = SA arr (if idx < hi then idx' else hi) where
    idx' = gallop (satisfies p . fst . (arr !)) idx hi
    (_, hi) = bounds arr

fromSortedArray :: Ord k => [(k, v)] -> SA k v
fromSortedArray l = SA (listArray (0, length l) l) 0


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
