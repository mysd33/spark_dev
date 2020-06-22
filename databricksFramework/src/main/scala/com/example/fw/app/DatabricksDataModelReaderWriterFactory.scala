package com.example.fw.app

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.infra.dataaccess.DatabricksDataModelReaderWriter

/**
 * DatabricksDataFileReaderWriter実装トレイトのDataFileReaderWriterを作成する
 * ファクトリオブジェクト
 */
object DatabricksDataModelReaderWriterFactory {
  /**
   * DataModelReaderWriterを作成する
   * @return DataModelReaderWriter
   */
  def createDataModelReaderWriter(): DataModelReaderWriter = {
    //Databricks用のReaderWriter
    new DataModelReaderWriter with DatabricksDataModelReaderWriter
  }
}
