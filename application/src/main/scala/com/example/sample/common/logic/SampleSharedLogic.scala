package com.example.sample.common.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.sample.model.Person
import org.apache.spark.sql.Dataset

//共通処理の例
class SampleSharedLogic(dataFileReaderWriter: DataFileReaderWriter) {
  def execute(ds: Dataset[Person]): Dataset[Person] = {
    // 実際には、DataFileReaderWriterを使ったデータアクセスを伴う共通処理
    // ここでは何にもしない
    ds
  }
}
