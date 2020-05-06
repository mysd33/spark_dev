package com.example.fw.app

import com.example.fw.domain.logic.Logic
import org.apache.spark.sql.SparkSession

/**
 * Databricks Notebookで動作させるためのエントリポイント
 *
 * {{{
 * import com.example.fw.app._
 * import com.example.sample.logic._
 *
 * //Create DataFileReaderWriter by DatabrickDataFileReaderWriterFactory
 * val readerWriter = DatabrickDataFileReaderWriterFactory.createDataFileReaderWriter()
 *
 * //Create Logic Class instance
 * val logic = new SampleDataSetBLogic(readerWriter)
 *
 * //run Logic Class
 * DatabricksNotebookEntryPoint.run(spark, logic)
 * }}}
 */
object DatabricksNotebookEntryPoint {
  def run(sparkSession: SparkSession, logic: Logic): Unit = {
    logic.execute(sparkSession)
  }
}
