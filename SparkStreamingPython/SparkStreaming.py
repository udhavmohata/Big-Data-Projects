from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open("/home/UDHAV.MAHATA/Documents/SparkStreaming/u.item", encoding = "ISO-8859-1") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 10)
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
    movieNames = loadMovieNames()
    #lines = KafkaUtils.createStream(ssc,"localhost:2181", "spark-streaming-consumer", {'kafkaPublish':1})
    lines = KafkaUtils.readStreams
    movies = lines.map(parseInput)
    movieDataset = spark.createDataFrame(movies)
    averageRatings = movieDataset.groupBy("movieID").avg("rating")
    counts = movieDataset.groupBy("movieID").count()
    averagesAndCounts = counts.join(averageRatings, "movieID")
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

    ssc.start()
    ssc.awaitTermination()
    