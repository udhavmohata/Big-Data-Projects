name := "SparkHive"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.19"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.5"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.1"
