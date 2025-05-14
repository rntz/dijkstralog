import Debug.Trace (trace)
import Control.Exception (assert)
import Data.List.NonEmpty (NonEmpty (..), head, tail, cons, uncons, nonEmpty)
import Data.Semigroup

-- Applicative without pure. A "semimonoidal functor".
class Functor f => Apply f where
  map2 :: (a -> b -> c) -> f a -> f b -> f c
  pair :: f a -> f b -> f (a,b)
  pair = map2 (,)

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

-- This has the same pre/post-conditions as `search`, but unlike search, exits
-- early if the post-conditions are already satisfied. This makes it idempotent;
-- in particular, it won't skip elements if called multiple times in a row. If
-- you want to drop the head element, use search instead.
seek :: Ord k => Seek k v -> k -> k -> Maybe (Seek k v)
seek t lo hi
  -- Exit early if our postconditions are already satisfied. This is necessary
  -- to avoid accidentally skipping elements.
  | lo <= lagging t && hi <= leading t = trace "lo <= lagging && hi <= leading" $
                                         Just t
  | otherwise = assert (lagging t <= lo && leading t <= hi) $ --check preconditions.
                search t lo hi

-- Intersects two seekable iterators.
instance Ord k => Apply (Seek k) where
  map2 :: Ord k => (a -> b -> c) -> Seek k a -> Seek k b -> Seek k c
  map2 f s t = Seek { position = map2 f (position s) (position t)
                    , search = if lagging s <= lagging t
                               then advance s t (map2 f)
                               else advance t s (flip (map2 f)) }
    -- Note the use of `search` for the first step. This forcibly "bumps" the
    -- lowest iterator even if it is at a matching key. This ensures we are
    -- searching the _rest_ of the sequence, excluding the current key-value
    -- pair (if we've found one).
    where advance s t f lo hi = do s' <- search s (lagging t) hi
                                   t' <- seek t (lo `min` lagging s') (leading s')
                                   seek (f s' t') lo (leading t')


-- Wrapping (Maybe (Seek k v)) into a newtype to give it useful instances.
newtype Iter k v = Iter (Maybe (Seek k v)) deriving Functor
instance Ord k => Apply (Iter k) where
  map2 f (Iter s) (Iter t) = Iter $ map2 f <$> s <*> t

-- Filters a seekable iterator, dropping some entries that don't match.
filterMap :: Eq k => (k -> a -> Maybe b) -> Iter k a -> Iter k b
filterMap f (Iter s) = Iter (loop <$> s)
  where loop (Seek posn search) = Seek posn' search'
          where search' lo hi = loop <$> search lo hi
                posn' = case posn of Range x y -> Range x y
                                     At k v -> maybe (Range k k) (At k) (f k v)

filter :: Eq k => (k -> v -> Bool) -> Iter k v -> Iter k v
filter test = filterMap (\k v -> if test k v then Just v else Nothing)


-- -- Union merge
-- dedup :: (Ord k, Semigroup v) => Iter k v -> Iter k v
-- dedup (Iter t) = Iter (loop <$> t)
--   where loop t@(Seek Range{} s) = t { search = \lo hi -> loop <$> s lo hi }
--         loop (Seek (At k v) s) = found k (v :| []) (s k k)
--         -- if we find a value, keep looking for ones at the same key until we
--         -- exhaust them.
--         found k vs Nothing = Seek (At k (sconcat vs)) (\_ _ -> Nothing)
--         found k vs (Just t)
--           | At k' v <- position t, k == k' = found k (cons v vs) (search t k k)
--           | k < leading t = Seek (At k (sconcat vs)) (\lo hi -> loop <$> search t lo hi)
--           | otherwise = found k vs (search t k k)

-- instance (Ord k, Semigroup v) => Semigroup (Iter k v) where
--   s <> t = dedup $ unionWithDuplicates s t

-- -- PROBLEM:
-- --
-- -- the assumption behind Position is that we only have a value if lo == hi.
-- --
-- -- unions break this assumption. we can have lagging & leading iterators and
-- -- still have a tuple. so what do lagging/leading _mean_ when we have unions?
-- --
-- -- perhaps lagging/leading are the wrong interface. When unions come into play,
-- -- do we want "advance lowest", or do we want "hit every iterator an equal
-- -- number of times" / "do one pass through all iterators"?
-- --
-- -- ONE ANSWER
-- --
-- -- A union will only ever hit its lowest sub-iterator, where lowest means lowest
-- -- _leading_ value. This means the union's lagging value may change
-- -- non-monotonically! Is this ok? IT VIOLATES CONTRACT OF SEARCH, RIGHT?
-- -- maybe we could make it ok if seek adjusted to be a no-op in that case?
-- unionWithDuplicates :: Ord k => Iter k v -> Iter k v -> Iter k v
-- unionWithDuplicates (Iter s) (Iter t) = outerJoin s t
--   where outerJoin Nothing Nothing = Nothing
--         outerJoin (Just s) Nothing = s
--         outerJoin Nothing (Just t) = t
--         outerJoin (Just s) (Just t) | leading s <= leading t = unionSeek s t
--                                     | otherwise = unionSeek t s
--         unionSeek s t =
--           case (position s, position t) of
--             At k v, Range lo hi -> assert (k <= hi) $
--                                    s { search = undefined }
--             At k v, At k' v' -> undefined --similar to previous case.
--             Range lo hi

-- -- instance (Ord k, Semigroup v) => Monoid (Iter k v) where mempty = Iter Nothing


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
m = pair <$> ms1 <*> ms2

xs = fromSorted [(x,x) | x <- [1,3 .. 100]]
ys = fromSorted [(y,y) | y <- [2,4 .. 100]]
zs = fromSorted [(z,z) | z <- [1,100]]

mxyz = map2 (,) <$> (map2 (,) <$> xs <*> ys) <*> zs
