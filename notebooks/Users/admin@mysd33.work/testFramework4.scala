// Databricks notebook source
import com.example.fw.app._
import com.example.sample.logic._


// COMMAND ----------

val readerWriter = DatabricksDataModelReaderWriterFactory.createDataModelReaderWriter()


// COMMAND ----------

val logic = new SampleDatasetBLogic4(readerWriter)

// COMMAND ----------

DatabricksNotebookEntryPoint.run(spark, logic)

// COMMAND ----------

