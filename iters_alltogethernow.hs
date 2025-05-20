import Prelude hiding (sum, product)
import Debug.Trace (trace)
import Data.Ord (comparing)
import Data.List (sortBy)

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

-- An iterator has either found a particular key-value pair, or knows a lower
-- bound on future keys.
--
-- ANNOYANCE: This is slightly too eager. We are forced to know whether we've
-- _found_ a value at a given key, rather than knowing what key we're at and
-- being able to force the computation of whether we've found a value. This
-- happens in fromFunction, for instance.
--
-- Of course, the computation of the value itself is still lazy, so this is
-- probably practically insignificant.
data Position k v = Found !k v
                  | Bound !(Bound k)
                    deriving (Show, Eq, Functor)

atleast :: Position k v -> Bound k
atleast (Found k _) = Atleast k
atleast (Bound k)   = k

data Iter k v = Iter
  { posn :: !(Position k v)
  , seek :: Bound k -> Iter k v
  } deriving Functor

bound :: Iter k v -> Bound k
bound = atleast . posn

emptyIter :: Iter k v
emptyIter = Iter (Bound Done) (const emptyIter)

-- -- Search with early cutoff. This is a constant-factor performance optimization
-- -- that helps for deep iterator trees with many leaves. The implementations of
-- -- `search` that we construct below (map2, outerJoin, fromFunction) do not check
-- -- whether we are _already_ at the given key. So if we didn't wrap recursive
-- -- calls in `seek`, any call to `search` would call search recursively on all
-- -- sub-iterators, all the way down to the leaves. Not clear if this is worth it.
-- seek :: Ord k => Iter k v -> Bound k -> Iter k v
-- seek t@(Iter (Found k v) _) target | matches target k = t
-- -- We check strict less-than here because we don't want to prevent doing useful
-- -- work toward finding the next key-value pair.
-- seek t@(Iter (Bound b)   _) target | target < b       = t
-- seek t target = search t target


-- Converting to & from sorted lists
toSorted :: Iter k v -> [(k,v)]
toSorted (Iter (Bound Done) _)   = []
toSorted (Iter (Found k v) seek) = (k,v) : toSorted (seek (Greater k))
toSorted (Iter (Bound k) seek)   = toSorted $ seek k

-- NB. We can't actually seek efficiently in a Haskell list.
fromSorted :: Ord k => [(k,v)] -> Iter k v
fromSorted []          = emptyIter
fromSorted l@((k,v):_) = Iter (Found k v) seek
  where seek target = fromSorted $ dropWhile (not . matches target . fst) l

matches :: Ord k => Bound k -> k -> Bool
matches Init        _ = True
matches Done        _ = False
matches (Atleast x) y = x <= y
matches (Greater x) y = x <  y

traceFromSorted :: (Show k, Ord k) => String -> [(k,v)] -> Iter k v
traceFromSorted name []          = emptyIter
traceFromSorted name l@((k,v):_) = Iter (Found kt v) seek where
  kt = trace (name ++ " " ++ show k) k
  seek target = traceFromSorted name $ dropWhile (not . matches target . fst) l

fromList :: Ord k => [(k,v)] -> Iter k v
traceFromList :: (Show k, Ord k) => String -> [(k,v)] -> Iter k v
fromList = fromSorted . sortBy (comparing fst)
traceFromList name = traceFromSorted name . sortBy (comparing fst)


-- Any function (k -> Maybe v) can become an unproductive Iter.
fromFunction :: (k -> Maybe v) -> Iter k v
fromFunction f = seek Init
  where seek k = Iter (at k) seek
        at (Atleast k) = case f k of
                           Just v  -> Found k v
                           Nothing -> Bound (Greater k)
        at p = Bound p

always :: v -> Iter k v
always x = fromFunction (\_ -> Just x)


-- Inner joins, ie generalized intersection.
instance Ord k => Applicative (Iter k) where
  pure = always
  liftA2 f s t = Iter posn' seek'
    where posn' | Found k x <- posn s, Found k' y <- posn t, k == k' = Found k (f x y)
                | otherwise = Bound (bound s `max` bound t)
          seek' k = liftA2 f s' t'
            where s' = seek s k
                  t' = seek t $ bound s' -- the leapfrog trick
          -- -- The simple implementation without sideways information passing.
          -- seek' k = map2 f (seek s k) (seek t k)


-- Outer joins, ie generalized union.
class Functor f => OuterJoin f where
  outerJoin :: (a -> c)      -- what to do if it's only in the left collection
            -> (b -> c)      -- what to do if it's only in the right collection
            -> (a -> b -> c) -- what to do if it's in both collections
            -> f a           -- the left collection
            -> f b           -- the right collection
            -> f c           -- the results

-- A value-aware minimum: we find the minimum position, and apply the
-- appropriate function (l, r, b, or nothing) to the values present.
instance Ord k => OuterJoin (Position k) where
  -- Concise (but inefficient?) implementation.
  outerJoin l r b p q = case (atleast p `compare` atleast q, p, q) of
      (LT, Found k x, _)         -> Found k (l x)
      (GT, _,         Found k y) -> Found k (r y)
      (EQ, Found k x, Found _ y) -> Found k (b x y)
      _                          -> Bound (atleast p `min` atleast q)

  -- -- Eliminating the repeated comparison.
  -- outerJoin l r b p q = case (atleast p `compare` atleast q, p, q) of
  --     (LT, Found k x, _)         -> Found k (l x)
  --     (GT, _, Found k y)         -> Found k (r y)
  --     (EQ, Found k x, Found _ y) -> Found k (b x y)
  --     (GT, _, _)                 -> Bound (atleast q)
  --     _                          -> Bound (atleast p)

  -- -- Most explicit.
  -- outerJoin l r b p q = case atleast p `compare` atleast q of
  --     LT -> l <$> p
  --     GT -> r <$> q
  --     EQ -> case (p, q) of
  --             -- If they're equal, we must wait until either both find a value
  --             -- or one exceeds the other.
  --             (Bound pos, _)         -> Bound pos
  --             (_, Bound pos)         -> Bound pos
  --             (Found k x, Found _ y) -> Found k (b x y)

instance Ord k => OuterJoin (Iter k) where
  outerJoin l r b s t = Iter (outerJoin l r b (posn s) (posn t))
                             (\k -> outerJoin l r b (seek s k) (seek t k))


-- Semiring magic.
class Additive a where
  zero :: a
  add :: a -> a -> a
  sum :: [a] -> a
  sum = foldr add zero

class Multiply a where
  one :: a
  mul :: a -> a -> a
  product :: [a] -> a
  product = foldr mul one

instance (Ord k, Additive a) => Additive (Iter k a) where
  zero = emptyIter
  add = outerJoin id id add

instance (Ord k, Multiply a) => Multiply (Iter k a) where
  one = pure one -- NB. unproductive
  mul = liftA2 mul

instance Additive Bool where zero = False; add = (||)
instance Multiply Bool where one  = True;  mul = (&&)

instance Additive Int where zero = 0; add = (+)
instance Multiply Int where one  = 1; mul = (*)

-- NB. contracting an unproductive iterator will infinite loop.
contract :: Additive a => Iter k a -> a
contract = sum . map snd . toSorted


-- EXAMPLES
instance (Show k, Show v) => Show (Iter k v) where
  show (Iter p _) = "Iter { posn = " ++ show p ++ " }"

list1 = [(1, "one"), (2, "two"), (3, "three"), (5, "five")]
list2 = [(1, "a"), (3, "c"), (5, "e")]

ms1, ms2 :: Iter Int String
ms1 = traceFromList "A" list1
ms2 = traceFromList "B" list2

m = liftA2 (,) ms1 ms2

xs = traceFromList "x" [(x,x) | x <- [1,3 .. 100]]
ys = traceFromList "y" [(y,y) | y <- [2,4 .. 100]]
zs = traceFromList "z" [(z,z) | z <- [1,100]]

mxyz :: Iter Int ((Int, Int), Int)
mxyz = liftA2 (,) (liftA2 (,) xs ys) zs

-- avoids interleaving of `trace` output with printing of value at REPL
drain x = length xs `seq` xs where xs = toSorted x


-- Let's try a triangle query!
-- R("a", 1). R("a", 2). R("b", 1). R("b", 2).
r :: Multiply a => [(String, [(Int, a)])]
r = [ ("a", [(1, one), (2, one)])
    , ("b", [(1, one), (2, one)])
    ]

-- It would be nice if I could do some type magic to handle nested lists.
-- Or maybe go direct from N-tuples into nested iterators?
rAB :: Multiply a => Iter String (Iter Int a)
rAB = traceFromList "R" $ map (fromList <$>) r

-- S(1, "one"). S(1, "wun"). S(2, "deux"). S(2, "two").
s :: Multiply a => [(Int, [(String, a)])]
s = [ (1, [("one", one), ("wun", one)])
    , (2, [("deux", one), ("two", one)])
    ]

sBC :: Multiply a => Iter Int (Iter String a)
sBC = traceFromList "S" $ map (fromList <$>) s

-- T("a", "one"). T("b", "deux"). T("mary", "mary").
t :: Multiply a => [(String, [(String, a)])]
t = [ ("a", [("one", one)])
    , ("b", [("deux", one)])
    , ("mary", [("mary", one)])
    ]

tAC :: Multiply a => Iter String (Iter String a)
tAC = traceFromList "T" $ map (fromList <$>) t

-- Bring them all into the same type by extending at the right columns.
-- Q(a,b,c) = R(a,b) and S(b,c) and T(a,c)
rABC :: Multiply a => Iter String (Iter Int (Iter k      a))
sABC :: Multiply a => Iter k      (Iter Int (Iter String a))
tABC :: Multiply a => Iter String (Iter k   (Iter String a))
qABC :: Multiply a => Iter String (Iter Int (Iter String a))
q    ::               Iter String (Iter Int (Iter String Int))

rABC = fmap (fmap always) rAB
sABC = always sBC
tABC = fmap always tAC

-- We expect the result:
--
--   Q("a", 1, "one")
--   Q("b", 2, "deux")
--
-- and this is what we see.
qABC = rABC `mul` sABC `mul` tABC
q    = qABC

-- try: do print qResults; putStrLn "----------"; mapM_ print qResults
qCount = contract $ contract $ contract q
qResults = drain $ fmap drain $ fmap (fmap drain) q -- ugh. need more type magic.
