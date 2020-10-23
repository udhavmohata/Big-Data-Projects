from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition
from pyspark.streaming.kafka import KafkaUtils, OffsetRange
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    ssc = StreamingContext(sc, 10)
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
    # kafkaParams = {"metadata.broker.list":"localhost:9092"}
    # start = 0
    # until = 10
    # partition = 0
    # topic = 'kafkaPublish'    
    # offset = OffsetRange(topic,partition,start,until)
    # offsets = [offset]
    # kvs = KafkaUtils.createRDD(sc,kafkaParams,offsets)
    kvs = KafkaUtils.createStream(ssc,"localhost:2181", "spark-streaming-consumer", {'kafkaPublish':1})
    # ipList = kvs.collect()
    # print(ipList)
    # rdd = sc.parallelize(ipList)
    # schemaDF = StructType([StructField('xyz',StringType(), True)])
    # df = spark.createDataFrame(rdd,schemaDF)
    # # print(df.schemaDF)
    # # lines = kvs.map(lambda x: x[1])
    # # counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

    df = spark.readStream.format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "SparkKafka")\
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")    
    
    
    query = df.writeStream.outputMode("append").format("console").start()

    query.awaitTermination()
    spark.stop()
    
