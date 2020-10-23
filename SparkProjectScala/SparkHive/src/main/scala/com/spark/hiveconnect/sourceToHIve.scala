package com.spark.hiveconnect

import java.io.File

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object sourceToHIve {
  case class Record(key: Int, value: String)
  def main(args: Array[String]){
    //val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example").master("local")
      .config("spark.sql.warehouse.dir", "/home/hadoopuser/metastore_db/spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH '/usr/local/spark3/examples/src/main/resources/kv1.txt' INTO TABLE src")
    sql("SELECT COUNT(*) FROM src").show()

    spark.stop()
  }

}
