package com.example.fw.domain.model

import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.sql.types.StructType


sealed abstract class DataFile[+T](path: String, val schema: Option[StructType] = None) {
  require(path != null && path.length > 0)
  val filePath = {
    //プロパティでベースパスの置き換え
    val basePath = ResourceBundleManager.get("basepath")
    basePath + path
  }
}

case class TextFileModel[T](path: String, schm: StructType = null) extends DataFile[T](path, Option(schm)) {
  //TODO: encoding
}

case class CsvModel[T](path: String, schm: StructType = null) extends DataFile[T](path, Option(schm)) {
  //TODO: encoding
}

case class JsonModel[T](path: String, schm: StructType = null) extends DataFile[T](path, Option(schm)) {
  //TODO: encoding
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
