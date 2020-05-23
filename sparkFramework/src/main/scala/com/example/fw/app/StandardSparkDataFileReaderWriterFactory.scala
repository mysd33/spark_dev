package com.example.fw.app

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.infra.dataaccess.StandardSparkDataFileReaderWriter

/**
 * StandardSparkDataFileReaderWriter実装トレイトのDataFileReaderWriterを作成する
 * ファクトリオブジェクト
 */
object StandardSparkDataFileReaderWriterFactory {
  /**
   * DataFileReaderWriterを作成する
   * DataFileReaderWriterの実装インスタンスを生成
   *
   * @return DataFileReaderWriter
   */
  def createDataFileReaderWriter(): DataFileReaderWriter = {
    //Sparkの標準のDataFileReaderWriterでDIして作成
    new DataFileReaderWriter with StandardSparkDataFileReaderWriter
  }
}
