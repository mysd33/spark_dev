package com.example.fw.domain.model

import org.apache.spark.sql.types.StructType


sealed abstract class DataFile[+T](val filePath: String, val schema: Option[StructType] = None) {
  //TODO: ベースパスの置き換えができるようにする
  require(filePath != null && filePath.length > 0)
}

case class CsvModel[T](path: String, schm: StructType = null) extends DataFile[T](path, Option(schm)) {
}

case class JsonModel[T](path: String, schm: StructType = null) extends DataFile[T](path, Option(schm)) {
}

case class ParquetModel[T](path: String, schm: StructType = null) extends DataFile[T](path, Option(schm)) {
}

case class MultiFormatCsvModel[T](path: String, schm: StructType = null) extends DataFile[T](path, Option(schm)) {
  //TODO:各行のCSVパースどこでやる？
  // https://stackoverflow.com/questions/25259425/spark-reading-files-using-different-delimiter-than-new-line
  //TODO:各行のCSV化どこでやる？
}

case class XmlModel[T](path: String, schm: StructType = null) extends DataFile[T](path, Option(schm)) {
  //TODO:XML形式にするのはどこでやる？
}
