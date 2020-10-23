from __future__ import print_function

import sys

from pyspark import SparkContext,SparkConf #Spark
from pyspark.streaming import StreamingContext #SparkStreaming
from pyspark.streaming.kafka import KafkaUtils #Kafka
from pyspark.sql import SparkSession

if __name__ == "__main__":
    conf = SparkConf().setAppName("RatingsHistogram")
    sc = SparkContext(conf = conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 3)
    topic = 'SparkPublish'
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
    kvs = KafkaUtils.createStream(ssc,"localhost:2181", "spark-streaming-consumer", {topic:1}) 
    # Direct Approach: Pulls data from Kafka 
    # kvs = KafkaUtils.createDirectStream(ssc, [topic],{'metadata.broker.list': 'localhost:9092'})
    # raw = kvs.flatMap(lambda s:[s])
    # message = raw.map(lambda x: x[1])
    # raw.pprint()
    # ssc.start()

    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe", "SparkPublish").load()
    
    query = df.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
    ssc.stop()
    