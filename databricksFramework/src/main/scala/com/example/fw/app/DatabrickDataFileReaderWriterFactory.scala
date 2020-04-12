package com.example.fw.app

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.infra.dataaccess.DatabricksDataFileReaderWriter

object DatabrickDataFileReaderWriterFactory {
  def createDataFileReaderWriter(): DataFileReaderWriter = {
    //Databricks用のReaderWriterに差し替え
    new DataFileReaderWriter with DatabricksDataFileReaderWriter
  }
}
