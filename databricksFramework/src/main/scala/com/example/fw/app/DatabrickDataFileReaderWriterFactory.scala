package com.example.fw.app

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.infra.dataaccess.DatabricksDataFileReaderWriter

/**
 * DatabrickDataFileReaderWriter実装トレイトのDataFileReaderWriterを作成する
 * ファクトリオブジェクト
 */
object DatabrickDataFileReaderWriterFactory {
  /**
   * DataFileReaderWriterを作成する
   * @return DataFileReaderWriter
   */
  def createDataFileReaderWriter(): DataFileReaderWriter = {
    //Databricks用のReaderWriter
    new DataFileReaderWriter with DatabricksDataFileReaderWriter
  }
}
