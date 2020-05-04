// Databricks notebook source
import com.example.fw.app._
import com.example.sample.logic._

// COMMAND ----------

val readerWriter = DatabrickDataFileReaderWriterFactory.createDataFileReaderWriter()

// COMMAND ----------

val logic = new SampleTokuteiXMLDataFrameBLogic(readerWriter)

//クラスタにspark-xml関連の依存jarがすべて入っていないと動作しない(spark-xml_2.11.0-0.9.0.jar, txw2-2.3.2.jar)
//val logic = new SampleXMLDatasetBLogic(readerWriter)
//val logic = new SampleTokuteiXMLDatasetBLogic(readerWriter)


// COMMAND ----------

DatabricksNotebookEntryPoint.run(spark, logic)

// COMMAND ----------

//dbutils.fs.ls("/mnt/mystorage/xml/")
//dbutils.fs.ls("/mnt/mystorage/tokutei/output/")
dbutils.fs.ls("/mnt/mystorage/tokutei/output2/")