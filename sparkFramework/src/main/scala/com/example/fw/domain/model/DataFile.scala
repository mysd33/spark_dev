package com.example.fw.domain.model

import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.sql.types.StructType

//TODO:caseクラスなので、本当はtraitがよい？
sealed abstract class DataFile[+T](path: String,
                                   val partition: Option[String] = None,
                                   val schema: Option[StructType] = None,
                                   val encoding: Option[String] = None) extends Serializable {
  require(path != null && path.length > 0)
  val filePath = {
    //プロパティでベースパスの置き換え
    val basePath = ResourceBundleManager.get("basepath")
    basePath + path
  }
}

case class TextFileModel[T](path: String, schm: StructType = null, ec: String = "UTF-8")
  extends DataFile[T](path, schema = Option(schm), encoding = Option(ec)) {
}

case class MultiFormatCsvModel[T](path: String, schm: StructType = null, ec: String = "UTF-8")
  extends DataFile[T](path, schema = Option(schm), encoding = Option(ec)) {
}

case class CsvModel[T](path: String, pt: String = null, schm: StructType = null, ec: String = "UTF-8")
  extends DataFile[T](path, Option(pt), Option(schm), Option(ec)) {
}

case class JsonModel[T](path: String, pt: String = null, schm: StructType = null, ec: String = "UTF-8")
  extends DataFile[T](path, Option(pt), Option(schm), Option(ec)) {
}

case class ParquetModel[T](path: String, pt: String = null, schm: StructType = null)
  extends DataFile[T](path, Option(pt), Option(schm)) {
}

case class XmlModel[T](path: String, schm: StructType = null, ec: String = "UTF-8")
  extends DataFile[T](path, schema = Option(schm), encoding = Option(ec)) {
  //TODO:XML形式にするのはどこでやる？
}
