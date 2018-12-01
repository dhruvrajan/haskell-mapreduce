import Prelude
import qualified Data.Map.Strict as Map

type MapFunction = String -> String -> [(String, String)]
type ReduceFunction = String -> [String] -> [String]
type MapReduceFunction = [(String, String)] -> [(String, [String])]

mymap :: MapFunction
myreduce :: ReduceFunction
mymapreduce :: MapReduceFunction


-- Word Counting [(Title, Document)]
mymap title document = map (\w -> (w, "1")) $ words document
myreduce word counts = [show $ sum . map (\x -> read x :: Int) $ counts]

_insertWith :: Ord k => (b -> a -> a) -> k -> b -> a -> Map.Map k a -> Map.Map k a
_insertWith f k b d mp =
  if not $ Map.member k mp
  then Map.insert k (f b d) mp
  else Map.adjust (f b) k mp

flatten :: [[a]] -> [a]         
flatten xs = (\z n -> foldr (\x y -> foldr z y x) n xs) (:) []  


_associate :: [(String, String)] -> Map.Map String [String] -> Map.Map String [String]
_associate [] _map = _map
_associate ((k, v):es) _map = _associate es $ _insertWith (:) k v [] _map

associate :: [(String, String)] -> [(String, [String])]
associate entries = Map.toList $ _associate entries Map.empty


mymapreduce input = map (\(k, v) -> (k, myreduce k v)) $ associate $ flatten $ map (\(k, v) -> mymap k v) input
