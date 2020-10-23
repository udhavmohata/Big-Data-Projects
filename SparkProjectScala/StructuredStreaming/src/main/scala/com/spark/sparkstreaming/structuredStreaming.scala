package com.spark.sparkstreaming

import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.util.regex.Pattern
import java.util.regex.Matcher
import java.text.SimpleDateFormat
import java.util.Locale

import Utilities._

object structuredStreaming {

  case class LogEntry(ip:String, client:String, user:String, dateTime:Timestamp, request:String, status:String, bytes:String, referer:String, agent:String)

  val logPattern: Pattern = apacheLogPattern()
  val datePattern: Pattern = Pattern.compile("\\[(.*?) .+]")

  def parseDateField(field: String): Option[Timestamp] = {

    val dateMatcher = datePattern.matcher(field)
    if (dateMatcher.find) {
      val dateString = dateMatcher.group(1)
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val date = (dateFormat.parse(dateString))
      val timestamp = new java.sql.Timestamp(date.getTime);
      return Option(timestamp)
    } else {
      None
    }
  }

  def parseLog(x:Row) : Option[LogEntry] = {

    val matcher:Matcher = logPattern.matcher(x.getString(0));
    if (matcher.matches()) {
      val timeString = matcher.group(4)
      return Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateField(matcher.group(4)).getOrElse(null),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)
      ))
    } else {
      return None
    }
  }

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("StructuredStreaming")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "/home/UDHAV.MAHATA/Documents/Checkpoints")
      .getOrCreate()

    setupLogging()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testing")
      .load()
    val rawData = df.selectExpr("CAST(value as STRING)")

    import spark.implicits._

    val structuredData = rawData.flatMap(parseLog).select("status", "dateTime")
    val windowed = structuredData.withWatermark("dateTime", "1 hour")
      .groupBy($"status",window($"dateTime", "1 hour")).count()
    val finalOp1 = windowed.selectExpr("CAST(window AS STRING)","CAST(status AS STRING)","CAST(count AS STRING)")
    finalOp1.createOrReplaceTempView("people")
    val joint = finalOp1.sqlContext.sql("SELECT CONCAT(window,'||',status,'||',count) AS status FROM people")
    val finalOp = joint.withColumnRenamed("status","value")



    val query = finalOp.select("value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "sink2")
      .start()
    query.awaitTermination()
    spark.stop()
  }

}
