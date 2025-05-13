-- BUGGY: toSorted m is wrong
import Debug.Trace (trace)
import Control.Exception (assert)

-- Applicative without pure. A "semimonoidal functor".
class Functor f => Apply f where
  map2 :: (a -> b -> c) -> f a -> f b -> f c

data Position k v = At k v | Range k k deriving Show
deriving instance Functor (Position k)

bounds :: Position k v -> (k, k)
bounds (Range lo hi) = (lo,hi)
bounds (At k v) = (k,k)

instance Ord k => Apply (Position k) where
  map2 f (At k1 x) (At k2 y) | k1 == k2 = At k1 (f x y)
  map2 f p q = Range (plo `min` qlo) (phi `max` qhi)
    where (plo, phi) = bounds p; (qlo, qhi) = bounds q

-- Sorted, seekable iterators as a coinductive type.
data Seek k v = Seek
  { position :: Position k v
  -- NB. searches the _rest_ of the sequence; if position is (At k v), search
  -- will not find the same (k,v) entry again. Use `seek` for a wrapper that is
  -- more idempotent.
  , search :: k -> k -> Maybe (Seek k v) }
  -- Let t' = search t lo hi. The pre/post conditions are:
  -- PRE:  lagging t <= lo               && leading t <= hi
  -- POST:              lo <= lagging t' &&              hi <= leading t'
deriving instance Functor (Seek k)

lagging, leading :: Seek k v -> k
lagging = fst . bounds . position
leading = snd . bounds . position

seek :: Ord k => Seek k v -> k -> k -> Maybe (Seek k v)
-- TODO: think carefully about exactly which early-exit cases we need here.
seek t lo hi
  -- Exit early if our postconditions are already satisfied. This is NECESSARY
  -- to avoid accidentally skipping elements; not having this caused a bug in a
  -- prior prototype. TODO: verify this.
  | lo <= lagging t && hi <= leading t = trace "lo <= lagging && hi <= leading" $ Just t
  -- If preconditions are not met, just return the same iterator.
  -- Do we need these? should they just assert?
  | lo < lagging t = assert False $ Just t
  | hi < leading t = assert False $ Just t --I don't think this case actually happens.
  | otherwise = search t lo hi

-- Intersects two seekable iterators.
instance Ord k => Apply (Seek k) where
  map2 :: Ord k => (a -> b -> c) -> Seek k a -> Seek k b -> Seek k c
  map2 f s t = Seek { position = map2 f (position s) (position t)
                    , search = if lagging s <= lagging t
                               then advance s t (map2 f)
                               else advance t s (flip (map2 f)) }
    -- TODO: I THINK THIS IS WRONG!
    -- why will it _stop_ when it hits a match?
    where advance s t f lo hi = do s' <- seek s (lagging t) hi
                                   t' <- seek t (lo `min` lagging s') (leading s')
                                   seek (f s' t') lo (leading t')

-- Filters a seekable iterator, dropping some entries that don't match.
filterMap :: Eq k => (k -> a -> Maybe b) -> Seek k a -> Seek k b
filterMap f (Seek posn search) = Seek posn' search'
  where search' lo hi = filterMap f <$> search lo hi
        posn' = case posn of Range x y -> Range x y
                             At k v -> maybe (Range k k) (At k) (f k v)


-- Converts an iterator to a sorted list.
toSorted :: Eq k => Maybe (Seek k v) -> [(k,v)]
toSorted Nothing = []
toSorted (Just (Seek (At k v) s)) = (k,v) : toSorted (s k k)
toSorted (Just (Seek (Range lo hi) s)) = toSorted (s hi hi)

-- Converts a sorted list to an iterator; seeking is obviously inefficient.
fromSorted :: Ord k => [(k,v)] -> Maybe (Seek k v)
fromSorted [] = Nothing
fromSorted ((k,v) : rest) = Just $ Seek (At k v) search
  where search lo hi = fromSorted $ dropWhile ((< hi) . fst) rest

traceFromSorted :: (Show k, Ord k) => String -> [(k,v)] -> Maybe (Seek k v)
traceFromSorted name [] = Nothing
traceFromSorted name ((k,v):rest) = Just $ Seek (At kt v) search
  where search lo hi = traceFromSorted name $ dropWhile ((< hi) . fst) rest
        kt = trace (name ++ " " ++ show k) k


-- Examples
list1 = [(1, "one"), (2, "two"), (3, "three"), (5, "five")]
list2 = [(1, "a"), (3, "c"), (5, "e")]

ms1 = traceFromSorted "A" list1
ms2 = traceFromSorted "B" list2
m = map2 (,) <$> ms1 <*> ms2

xs = fromSorted [(x,x) | x <- [1,3 .. 100]]
ys = fromSorted [(y,y) | y <- [2,4 .. 100]]
zs = fromSorted [(z,z) | z <- [1,100]]

mxyz = map2 (,) <$> (map2 (,) <$> xs <*> ys) <*> zs
