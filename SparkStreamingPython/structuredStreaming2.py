import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract

os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/UDHAV.MAHATA/Documents/SparkSQL/mysql-connector-java-5.1.45-bin.jar  pyspark-shell'

# Create a SparkSession
spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
accessLines = spark.readStream.text("/home/UDHAV.MAHATA/Documents/logs")

# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

statusCountsDF = logsDF.groupBy(logsDF.status).count()

# query1 = (statusCountsDF.writeStream.format("jdbc").start("jdbc:mysql://localhost:3306/demo?user=root&password=password").start())

# Writes data to console window
query = ( statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start() )
query.awaitTermination()




