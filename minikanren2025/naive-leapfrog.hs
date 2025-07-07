data Iter k v = Empty
              | Yield k v (k -> Iter k v)

fromSorted :: Ord k => [(k, v)] -> Iter k v
fromSorted [] = Empty
fromSorted ((k,v) : rest) = Yield k v seek
  where seek k' = fromSorted (dropWhile ((< k') . fst) rest)

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

evens = fromSorted [(x, "even") | x <- [0, 2 .. 7_777_777]]
odds  = fromSorted [(x, "odd")  | x <- [1, 3 .. 7_777_777]]
ends  = fromSorted [(x, "end")  | x <- [0,      7_777_777]]
