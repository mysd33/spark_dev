// Databricks notebook source
val sc = spark.sparkContext
sc.setLogLevel("WARN")

val textFile = sc.textFile("dbfs:/databricks-datasets/samples/docs/README.md")
val words = textFile.flatMap(line => line.split(" "))
val wordCounts = words.map(word => (word, 1)).reduceByKey((a,b) => a+b)
//println(wordCounts)
wordCounts.collect().foreach(println)

//wordCounts.saveAsTextFile("dbfs:/temp/")

// COMMAND ----------

