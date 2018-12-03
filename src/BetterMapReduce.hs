{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE FlexibleContexts #-}

import qualified Data.Map.Strict as Map
import qualified Data.List as List
import Data.Ord (comparing)

type MapFunction a k v = Ord k => a -> [(k, v)]
type ShuffleFunction f k v = (Ord k, Foldable f) =>  [(k, v)] -> [(k, f v)]
type ReduceFunction f k v v' = (Ord k, Foldable f) => k -> f v -> f v'
type MapReduceFunction a k v f v'= (Ord k, Foldable f) => MapFunction a k v -> ReduceFunction f k v v'-> [a] -> [(k, f v')]

groupBy :: (a -> a -> Bool) -> (a -> b -> b) -> b -> [a] -> [b]
groupBy _ _ _ [] = []
groupBy eq group d (x:xs) = group x ys : groupBy eq group d zs
  where
    (ys', zs) = List.span (eq x) xs
    ys = foldr group d ys'

shuffle :: (Ord ) k => ShuffleFunction [] k v
shuffle ((ek,ev):es) = groupBy (\x y -> fst x == fst y) (\x y -> (fst x, snd x : snd y)) (ek, []) $ List.sortBy (comparing fst) ((ek, ev):es)

mapReduce :: (Ord k) =>  MapReduceFunction a k v [] v'
mapReduce mapFunction reduceFunction records = map (\(k, v) -> (k, reduceFunction k v)) $ shuffle $ records >>= mapFunction

-- Word Counting
mapFunction :: MapFunction String String Int
mapFunction document = map (\w -> (w, 1)) $ words document

reduceFunction :: ReduceFunction [] String Int Int
reduceFunction word counts = [sum counts]

wordCounts = mapReduce mapFunction reduceFunction

counts = wordCounts ["the dog is running in the yard", "the dog hates the cat", "the cat runs around the yard"]


-- Distributed Grep
grepMap :: (String -> Bool) -> MapFunction (String, String) String (Int, String)
grepMap matches (title, document) = filter (matches . snd . snd) $ zip (repeat title) $ zip [1..] (lines document)

grepReduce :: ReduceFunction [] String (Int, String) (Int, String)
grepReduce title = id

grep matches = mapReduce (grepMap matches) grepReduce

-- Inverted Index
invertedIndexMap :: MapFunction (String, String) String String
invertedIndexMap (title, document) = zip (words document) (repeat title)

invertedIndexReduce :: ReduceFunction [] String String String
invertedIndexReduce word = id

invertedIndex = mapReduce invertedIndexMap invertedIndexReduce
