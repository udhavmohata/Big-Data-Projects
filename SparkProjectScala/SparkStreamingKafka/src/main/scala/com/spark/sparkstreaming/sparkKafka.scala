package com.spark.sparkstreaming

  import org.apache.spark.sql.SparkSession


object sparkKafka {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "/home/UDHAV.MAHATA/Documents/Checkpoints")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testing")
      .load()
    val ds = df.selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val upper = ds.map(_.toString().toUpperCase)

    val query = upper
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "sink")
      .start()

    query.awaitTermination()
  }

}
