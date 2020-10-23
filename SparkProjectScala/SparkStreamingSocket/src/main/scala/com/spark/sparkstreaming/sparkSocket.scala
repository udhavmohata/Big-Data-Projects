package com.spark.sparkstreaming

import org.apache.spark.sql.SparkSession

object sparkSocket {
  def main(args: Array[String]){
    val spark = SparkSession.builder()
      .appName("SocketReading")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val socketDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()

    val wordsDF = socketDF.as[String].flatMap(_.split(" "))
    val capitalDF = wordsDF.map(_.toUpperCase)
    val wordCounts = capitalDF.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}
