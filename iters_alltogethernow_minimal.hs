data Iter k v = Iter { posn :: Position k v
                     , seek :: Bound k -> Iter k v
                     } deriving Functor
key :: Iter k v -> Bound k
key = atleast . posn

-- An iterator has either found a particular key-value pair, or knows a lower
-- bound on future keys.
data Position k v = Found !k v | Bound !(Bound k) deriving (Show, Eq, Functor)
atleast :: Position k v -> Bound k
atleast (Found k _) = Atleast k
atleast (Bound p)   = p

-- A lower bound in a totally ordered key-space k; corresponds to some part of an
-- ordered sequence we can seek forward to.
data Bound k = Init | Atleast !k | Greater !k | Done deriving (Show, Eq)
instance Ord k => Ord (Bound k) where
  Init      <= _         = True
  _         <= Done      = True
  _         <= Init      = False
  Done      <= _         = False
  Atleast x <= Atleast y = x <= y
  Atleast x <= Greater y = x <= y
  Greater x <= Atleast y = x <  y -- <== NB. the odd one out!
  Greater x <= Greater y = x <= y

-- Any key-value function can be an iterator - just not a productive one.
fromFunction :: (k -> Maybe v) -> Iter k v
fromFunction f = seek Init
  where seek k = Iter (at k) seek
        at (Atleast k) = maybe (Bound (Greater k)) (Found k) (f k)
        at p           = Bound p

-- Inner joins, ie generalized intersection. liftA2 is productive if either argument is.
instance Ord k => Applicative (Iter k) where
  pure x = fromFunction (\_ -> Just x)
  liftA2 f s t = Iter posn' seek'
    where posn' | Found k x <- posn s, Found k' y <- posn t, k == k' = Found k (f x y)
                | otherwise                                          = Bound (key s `max` key t)
          seek' k = liftA2 f s' t'
            where s' = seek s k
                  t' = seek t $ key s' -- the leapfrog trick
          -- -- Simpler implementation with same asymptotics:
          -- seek' k = liftA2 f (seek s k) (seek t k)

-- Outer joins, ie generalized union.
class Functor f => OuterJoin f where
  outerJoin :: (a -> c) -> (b -> c) -> (a -> b -> c) -> f a -> f b -> f c

-- value-aware minimum of two positions
instance Ord k => OuterJoin (Position k) where
  outerJoin l r b p q = case (atleast p `compare` atleast q, p, q) of
      (LT, Found k x, _)         -> Found k (l x)
      (GT, _,         Found k y) -> Found k (r y)
      (EQ, Found k x, Found _ y) -> Found k (b x y)
      _                          -> Bound (atleast p `min` atleast q)

instance Ord k => OuterJoin (Iter k) where
  -- outerJoin is productive if both arguments are.
  outerJoin l r b s t = Iter (outerJoin l r b (posn s) (posn t))
                             (\k -> outerJoin l r b (seek s k) (seek t k))



-- NB. We can't actually seek efficiently in a Haskell list.
fromSorted :: Ord k => [(k,v)] -> Iter k v
fromSorted []          = emptyIter
fromSorted l@((k,v):_) = Iter (Found k v) seek
  where seek target = fromSorted $ dropWhile (not . matches target . fst) l

toSorted :: Iter k v -> [(k,v)]
toSorted (Iter (Bound Done) _)   = []
toSorted (Iter (Found k v) seek) = (k,v) : toSorted (seek (Greater k))
toSorted (Iter (Bound k) seek)   = toSorted $ seek k

emptyIter = Iter (Bound Done) (const emptyIter)

matches :: Ord k => Bound k -> k -> Bool
matches Init        _ = True
matches Done        _ = False
matches (Atleast x) y = x <= y
matches (Greater x) y = x <  y

list1 = [(1, "one"), (2, "two"), (3, "three"), (5, "five")]
list2 = [(1, "a"), (3, "c"), (5, "e")]

ms1, ms2 :: Iter Int String
ms1 = fromSorted list1
ms2 = fromSorted list2
m = (,) <$> ms1 <*> ms2

xs = fromSorted [(x,x) | x <- [1,3 .. 100]]
ys = fromSorted [(y,y) | y <- [2,4 .. 100]]
zs = fromSorted [(z,z) | z <- [1,100]]

mxyz :: Iter Int ((Int, Int), Int)
mxyz = (,) <$> ((,) <$> xs <*> ys) <*> zs

