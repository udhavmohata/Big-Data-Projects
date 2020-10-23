package com.spark.sparkstreaming

import java.util.regex.Pattern

import org.apache.spark.sql.SparkSession

object logToKafka {
  def apacheLogPattern():Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }
  def main(args: Array[String]){
    val spark = SparkSession.builder()
      .appName("logKafka")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "/home/UDHAV.MAHATA/Documents/Checkpoints")
      .getOrCreate()

    val accessLines = spark.readStream.text("/home/UDHAV.MAHATA/Documents/Spark/logs")


  }
}
