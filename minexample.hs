{-# LANGUAGE FunctionalDependencies #-}
class Seek1 iter k v | iter -> k, iter -> v where
data Merge iter = Merge [iter]
instance Seek1 iter k v => Seek1 (Merge iter) k v where
