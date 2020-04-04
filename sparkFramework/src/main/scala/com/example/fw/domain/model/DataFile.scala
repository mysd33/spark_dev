package com.example.fw.domain.model

import org.apache.spark.sql.Row

sealed abstract class DataFile[T](path: String) {
  //TODO: ベースパスの置き換えができるようにする
  require(path != null && path.length > 0)
  val filePath: String = path
}

case class CsvModel(path: String) extends DataFile[Row](path: String) {
}

case class JsonModel(path: String) extends DataFile[Row](path: String) {
}

case class ParquetModel(path: String) extends DataFile[Row](path: String) {
}

case class MultiFormatCsvModel(path: String) extends DataFile[Row](path: String) {
  //TODO:各行のCSVパースどこでやる？
  // https://stackoverflow.com/questions/25259425/spark-reading-files-using-different-delimiter-than-new-line
  //TODO:各行のCSV化どこでやる？
}

case class XmlModel(path: String) extends DataFile[Row](path: String) {
  //TODO:XML形式にするのはどこでやる？
}
