// Databricks notebook source
import com.example.fw.app._
import com.example.sample.logic._

// COMMAND ----------

val readerWriter = DatabrickDataFileReaderWriterFactory.createDataFileReaderWriter()

// COMMAND ----------

val logic = new SampleMedReceiptRDDBLogic(readerWriter)

// COMMAND ----------

DatabricksNotebookEntryPoint.run(spark, logic)

// COMMAND ----------

dbutils.fs.ls("/mnt/mystorage/receipt/output/MN")

// COMMAND ----------

dbutils.fs.ls("/mnt/mystorage/receipt/output/RE")

// COMMAND ----------

