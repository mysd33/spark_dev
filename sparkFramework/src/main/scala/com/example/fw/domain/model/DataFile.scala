package com.example.fw.domain.model

import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.sql.types.StructType

//TODO:caseクラスなので、本当はtraitがよい？
sealed abstract class DataFile[+T](path: String, val schema: Option[StructType] = None, val encoding: String)
extends Serializable {
  require(path != null && path.length > 0)
  val filePath = {
    //プロパティでベースパスの置き換え
    val basePath = ResourceBundleManager.get("basepath")
    basePath + path
  }
}

case class TextFileModel[T](path: String, schm: StructType = null, ec: String = "UTF-8")
  extends DataFile[T](path, Option(schm), ec) {
  //TODO: partitioning対応
}

case class CsvModel[T](path: String, schm: StructType = null, ec: String = "UTF-8")
  extends DataFile[T](path, Option(schm), ec) {
  //TODO: partitioning対応
}

case class JsonModel[T](path: String, schm: StructType = null, ec: String = "UTF-8")
  extends DataFile[T](path, Option(schm), ec) {
  //TODO: partitioning対応
}

case class ParquetModel[T](path: String, schm: StructType = null, ec: String = "UTF-8")
  extends DataFile[T](path, Option(schm) ,ec) {
  //TODO: partitioning対応
}

case class MultiFormatCsvModel[T](path: String, schm: StructType = null, ec: String = "UTF-8")
  extends DataFile[T](path, Option(schm), ec) {
}

case class XmlModel[T](path: String, schm: StructType = null, ec: String = "UTF-8")
  extends DataFile[T](path, Option(schm), ec) {
  //TODO:XML形式にするのはどこでやる？
}
