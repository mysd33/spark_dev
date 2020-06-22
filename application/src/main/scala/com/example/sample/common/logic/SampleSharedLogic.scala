package com.example.sample.common.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.sample.common.entity.Person
import org.apache.spark.sql.Dataset

/**
 * 共通処理の例
 *
 * @param dataModelReaderWriter
 */
class SampleSharedLogic(dataModelReaderWriter: DataModelReaderWriter) {
  def execute(ds: Dataset[Person]): Dataset[Person] = {
    // 実際には、DataModelReaderWriterを使ったデータアクセスを伴う共通処理
    // ここでは何にもしない
    ds
  }
}
