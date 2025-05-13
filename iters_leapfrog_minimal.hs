-- BUGGY: toSorted m is missing 3

-- Sorted, seekable iterators as a coinductive type.
data Seek k v = Seek
  { lagging :: !k
  , leading :: !k
  -- invariant: lagging <= leading
  , value :: v
  -- value must only be accessed when lagging == leading.
  , search :: k -> k -> Maybe (Seek k v)
  -- Let t' = search t lo hi. The pre/post conditions are:
  -- PRE:  lagging t <= lo               && leading t <= hi
  -- POST:              lo <= lagging t' &&              hi <= leading t'
  }
deriving instance Functor (Seek k)

seek :: Ord k => Seek k v -> k -> k -> Maybe (Seek k v)
seek t lo hi | lo < lagging t || hi < leading t = Just t
             | otherwise = search t lo hi

-- Intersects two seekable iterators.
map2 :: Ord k => (a -> b -> c) -> Seek k a -> Seek k b -> Seek k c
map2 f s t = Seek { lagging = lagging s `min` lagging t
                  , leading = leading s `max` leading t
                  , value = f (value s) (value t)
                  , search = if lagging s <= lagging t
                             then advance s t (map2 f)
                             else advance t s (flip (map2 f)) }
  where advance s t f lo hi = do s' <- seek s (lagging t) hi
                                 t' <- seek t (lo `min` lagging s') (leading s')
                                 seek (f s' t') lo (leading t')

-- Filters a seekable iterator. Kinda hacky implementation.
filterMap :: Eq k => (k -> a -> Maybe b) -> Seek k a -> Maybe (Seek k b)
filterMap f t | k == lagging t, Nothing <- v' = filterMap f =<< search t k k
              | otherwise = Just $ t { value = (let Just x = v' in x)
                                     , search = \lo hi -> filterMap f =<< search t lo hi }
  where k = leading t
        v' = f k (value t)


-- Converts an iterator to a sorted list.
toSorted :: Eq k => Maybe (Seek k v) -> [(k,v)]
toSorted Nothing = []
toSorted (Just s) = [(k, value s) | k == lagging s] ++ toSorted (search s k k)
  where k = leading s

-- Converts a sorted list to an iterator; seeking is obviously inefficient.
fromSorted :: Ord k => [(k,v)] -> Maybe (Seek k v)
fromSorted [] = Nothing
fromSorted ((k,v) : rest) = Just $ Seek
                            { lagging = k, leading = k, value = v
                            , search = \lo hi -> fromSorted $ dropWhile ((< hi) . fst) rest }


-- Examples
list1 = [(1, "one"), (2, "two"), (3, "three"), (5, "five")]
list2 = [(1, "a"), (3, "c"), (5, "e")]

ms1 = fromSorted list1
ms2 = fromSorted list2
m = map2 (,) <$> ms1 <*> ms2

xs = fromSorted [(x,x) | x <- [1,3 .. 100]]
ys = fromSorted [(y,y) | y <- [2,4 .. 100]]
zs = fromSorted [(z,z) | z <- [1,100]]

mxyz = map2 (,) <$> (map2 (,) <$> xs <*> ys) <*> zs
