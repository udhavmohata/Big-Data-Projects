package com.spark.sparkstreaming

import org.apache.spark.sql.SparkSession

object kafkaSocket {
  def main(args: Array[String]){
    val spark = SparkSession.builder()
      .appName("StreamingKafkaSocket")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    //Reading data from socket
    val socketDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()
    //Reading data from kafka
    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testing")
      .load()


    //processing data from socket
    val sWordsDF = socketDF.as[String].flatMap(_.split(" "))
    val sCapitalDF = sWordsDF.map(_.toUpperCase)
    val sWordCounts = sCapitalDF.groupBy("value").count()

    //processing data from from kafka
    val kWordsDF = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]
    val kSplitDF = kWordsDF.flatMap(_.split(" "))
    val kCapitalDF = kSplitDF.map(_.toUpperCase)
    val kWordCounts = kCapitalDF.groupBy("value").count()

    //Send socket data to target
    val sQuery = sWordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    //Send kafka data to target
    val kQuery = kWordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()


    kQuery.awaitTermination()
    sQuery.awaitTermination()
  }

}
