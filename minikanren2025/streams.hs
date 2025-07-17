data Stream a = Empty
              | Yield a (Stream a)
              | Later   (Stream a)
                deriving Show

union Empty        ys = ys
union (Yield x xs) ys = Yield x (union xs ys)  -- keep focus on xs
union (Later xs)   ys = Later   (union ys xs)  -- swap focus to ys

theAnswer :: Stream Int
theAnswer = union (Later theAnswer) (Yield 42 Empty)
