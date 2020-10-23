package com.spark.hiveconnect

import org.apache.spark.sql.SparkSession

object mySQLToHive {
  def main(args: Array[String]){

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/demo")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "data")
      .option("user", "root")
      .option("password", "password")
      .load()

    df.show()

  }
}
