data Iter k v = Iter { posn :: Position k v , seek :: Bound k -> Iter k v } deriving Functor
data Position k v = Found !k v | Bound !(Bound k) deriving (Show, Eq, Functor)
data Bound k = Init | Atleast !k | Greater !k | Done deriving (Show, Eq)

bound :: Iter k v -> Bound k
bound (Iter (Found k _) _) = Atleast k
bound (Iter (Bound p)   _) = p

instance Ord k => Ord (Bound k) where
  compare x y = embed x `compare` embed y
    where embed Init        = (0, Nothing)
          embed (Atleast k) = (1, Just (k, 0))
          embed (Greater k) = (1, Just (k, 1))
          embed Done        = (2, Nothing)

join :: Ord k => Maybe (a -> c) -> Maybe (b -> c) -> (a -> b -> c)
     -> Iter k a -> Iter k b -> Iter k c
join l r b s t = Iter posn' seek'
  where posn' = case bound s `compare` bound t of
                  LT -> maybe (Bound $ bound t) (<$> posn s) l
                  GT -> maybe (Bound $ bound s) (<$> posn t) r
                  EQ | Found k x <- posn s, Found _ y <- posn t -> Found k (b x y)
                     | otherwise -> Bound (bound s)
        seek' k = join l r b s' t'
          where (s', t') | Nothing <- r = (seek s k,          seek t (bound s'))
                         | Nothing <- l = (seek s (bound t'), seek t k)
                         | otherwise    = (seek s k,          seek t k)

fromSorted :: Ord k => [(k,v)] -> Iter k v
fromSorted l = Iter posn seek
  where posn = case l of [] -> Bound Done; ((k,v):_) -> Found k v
        seek target = fromSorted $ dropWhile ((target >) . Atleast . fst) l

toSorted :: Iter k v -> [(k,v)]
toSorted (Iter (Bound Done) _)   = []
toSorted (Iter (Found k v) seek) = (k,v) : toSorted (seek (Greater k))
toSorted (Iter (Bound k) seek)   = toSorted $ seek k
