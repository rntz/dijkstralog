data Position k v = Found k v | Bound (Bound k)
data Bound k = Atleast k | Greater k | Done deriving Eq

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
toSorted (Seek (Found k v) seek) = (k,v) : toSorted (seek (Greater k))
toSorted (Seek (Bound k)   seek) = toSorted (seek k)

fromSorted :: Ord k => [(k,v)] -> Seek k v
fromSorted l = Seek posn seek
  where posn = case l of [] -> Bound Done
                         (k,v):_ -> Found k v
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

evens = fromSorted [(x, "even") | x <- [0, 2 .. 87_777_777]]
odds  = fromSorted [(x, "odd")  | x <- [1, 3 .. 87_777_777]]
ends  = fromSorted [(x, "end")  | x <- [0,      87_777_777]]

main = print $ length $ toSorted $ evens `intersect` odds `intersect` ends
