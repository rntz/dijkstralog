data Iter k v = Iter { posn :: Position k v
                     , seek :: Bound k -> Iter k v
                     } deriving Functor
bound :: Iter k v -> Bound k
bound = atleast . posn

-- An iterator has either found a particular key-value pair, or knows a lower
-- bound on future keys.
data Position k v = Found !k v | Bound !(Bound k) deriving (Show, Eq, Functor)
atleast :: Position k v -> Bound k
atleast (Found k _) = Atleast k
atleast (Bound p)   = p

-- A generic way of giving lower bounds on a totally ordered key space.
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

-- Generic joins: outer, inner, left-outer, right-outer.
class Functor f => Joinable f where
  join :: Maybe (a -> c) -> Maybe (b -> c) -> (a -> b -> c) -> f a -> f b -> f c

instance Ord k => Joinable (Iter k) where
  join l r b s t = Iter (join l r b (posn s) (posn t)) seek'
    where seek' k = join l r b s' t'
            -- The first two cases are sideways information passing optimizations: if we don't need
            -- every key in t, we can seek it to the bound we get after seeking s, and vice-versa.
            where (s', t') | Nothing <- r = (seek s k,          seek t (bound s'))
                           | Nothing <- l = (seek s (bound t'), seek t k)
                           | otherwise    = (seek s k,          seek t k)

instance Ord k => Joinable (Position k) where
  join ifleft ifright ifboth p q = case (atleast p `compare` atleast q, p, q) of
    (EQ, Found k x, Found _ y)             -> Found k (ifboth x y)
    (LT, Found k x, _) | Just f <- ifleft  -> Found k (f x)
    (GT, _, Found k y) | Just f <- ifright -> Found k (f y)
    _ -> Bound $ case (ifleft, ifright) of
                   (Just _,  Just _)  -> atleast p `min` atleast q -- full outer join
                   (Nothing, Nothing) -> atleast p `max` atleast q -- inner join
                   (Just _,  Nothing) -> atleast p                 -- want everything from p
                   (Nothing, Just _)  -> atleast q                 -- want everything from q

instance Ord k => Applicative (Iter k) where
  pure x = fromFunction (\_ -> Just x)
  liftA2 = join Nothing Nothing


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

