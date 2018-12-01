import qualified Data.Map.Strict as Map


type MapFunction a k v = a -> [(k, v)]
type ShuffleFunction f k v = [(k, v)] -> [(k, f v)]
type ReduceFunction f k v v' = k -> f v -> f v'
type MapReduceFunction a k v f v'= MapFunction a k v -> ReduceFunction f k v v'-> [a] -> [(k, f v')]

_insertWith :: Ord k => (b -> a -> a) -> k -> b -> a -> Map.Map k a -> Map.Map k a
_insertWith f k b d mp =
  if not $ Map.member k mp
  then Map.insert k (f b d) mp
  else Map.adjust (f b) k mp

_shuffleList :: (Ord k) =>  [(k, v)] -> Map.Map k [v] -> Map.Map k [v]
_shuffleList []  mp = mp
_shuffleList ((k, v):entries) mp = _shuffleList entries $ _insertWith (:) k v [] mp

shuffleList :: (Ord k) => ShuffleFunction [] k v
shuffleList entries = Map.toList $ _shuffleList entries Map.empty

flatten :: [[a]] -> [a]         
flatten xs = (\z n -> foldr (\x y -> foldr z y x) n xs) (:) []  

mapReduce :: (Ord k) =>  MapReduceFunction a k v [] v'
mapReduce mapFunction reduceFunction records = map (\(k, v) -> (k, reduceFunction k v)) $ shuffleList $ flatten $ map mapFunction records


-- Word Counting
mapFunction :: MapFunction String String Int
mapFunction document = map (\w -> (w, 1)) $ words document

reduceFunction :: ReduceFunction [] String Int Int
reduceFunction word counts = [sum counts]

wordCounts = mapReduce mapFunction reduceFunction

counts = wordCounts ["the dog is running in the yard", "the dog hates the cat", "the cat runs around the yard"]

