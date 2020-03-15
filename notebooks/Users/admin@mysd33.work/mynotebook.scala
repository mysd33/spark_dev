// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

val events = spark.read
  .option("inferSchema", "true")
  .json("/databricks-datasets/structured-streaming/events/")
  .withColumn("date", expr("time"))
  .drop("time")
  .withColumn("date", from_unixtime($"date", "yyyy-MM-dd"))
display(events)



// COMMAND ----------

import org.apache.spark.sql.SaveMode

events.write.format("delta").mode(SaveMode.Overwrite).partitionBy("date").save("/delta/events/")

// COMMAND ----------

val events_delta = spark.read.format("delta").load("/delta/events/")
display(events_delta)

// COMMAND ----------

display(spark.sql("DROP TABLE IF EXISTS events"))

display(spark.sql("CREATE TABLE events USING DELTA LOCATION '/delta/events/'"))

// COMMAND ----------

events_delta.count()

// COMMAND ----------

display(spark.sql("SELECT count(*) FROM events"))

// COMMAND ----------

display(events_delta.groupBy("action", "date").agg(count("action").alias("action_count")).orderBy("date", "action"))

// COMMAND ----------

val historical_events = spark.read 
  .option("inferSchema", "true") 
  .json("/databricks-datasets/structured-streaming/events/") 
  .withColumn("date", expr("time-172800")) 
  .drop("time")
  .withColumn("date", from_unixtime($"date", "yyyy-MM-dd"))

// COMMAND ----------

historical_events.write.format("delta").mode("append").partitionBy("date").save("/delta/events/")

// COMMAND ----------

events_delta.count()

// COMMAND ----------

display(events_delta.groupBy("action", "date").agg(count("action").alias("action_count")).orderBy("date", "action"))

// COMMAND ----------

dbutils.fs.ls("dbfs:/delta/events/date=2016-07-25/")

// COMMAND ----------

dbutils.fs.ls("dbfs:/delta/events/")

// COMMAND ----------

display(spark.sql("OPTIMIZE events"))

// COMMAND ----------

display(spark.sql("DESCRIBE HISTORY events"))

// COMMAND ----------

display(spark.sql("DESCRIBE FORMATTED events"))

// COMMAND ----------

