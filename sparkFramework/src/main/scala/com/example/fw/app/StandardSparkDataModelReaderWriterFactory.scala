package com.example.fw.app

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.infra.dataaccess.StandardSparkDataModelReaderWriter

/**
 * StandardSparkDataModelReaderWriter実装トレイトのDataFileReaderWriterを作成する
 * ファクトリオブジェクト
 */
object StandardSparkDataModelReaderWriterFactory {
  /**
   * DataModelReaderWriterを作成する
   * DataModelReaderWriterの実装インスタンスを生成
   *
   * @return DataModelReaderWriter
   */
  def createDataModelReaderWriter(): DataModelReaderWriter = {
    //Sparkの標準のDataFileReaderWriterでDIして作成
    new DataModelReaderWriter with StandardSparkDataModelReaderWriter
  }
}
