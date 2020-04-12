package com.example.fw.app

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.Logic
import com.example.fw.infra.dataaccess.DatabricksDataFileReaderWriter
import org.apache.spark.sql.SparkSession

object DatabricksNotebookEntryPoint {
  def run(sparkSession: SparkSession, logic: Logic): Unit = {
    logic.execute(sparkSession)
  }
}
