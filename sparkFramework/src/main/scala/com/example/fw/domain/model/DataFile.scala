package com.example.fw.domain.model

import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.sql.types.StructType

sealed trait DataFile[+T] extends Serializable {
  require(path != null)
  val path: String
  val filePath: String = {
    //プロパティでベースパスの置き換え
    val basePath = ResourceBundleManager.get("basepath")
    basePath + path
  }
  val partition: Option[String] = None
  val schema: Option[StructType] = None
  val encoding: Option[String] = None
}


case class TextFileModel[T](override val path: String,
                            override val schema: Option[StructType] = None,
                            override val encoding: Option[String] = Some("UTF-8")) extends DataFile[T]

case class MultiFormatCsvModel[T](override val path: String,
                                  delimiter: Option[String] = Some("\u0000"),
                                  override val schema: Option[StructType] = None,
                                  override val encoding: Option[String] = Some("UTF-8")) extends DataFile[T]

case class CsvModel[T](override val path: String,
                       delimiter: Option[String] = Some(","),
                       override val partition: Option[String] = None,
                       override val schema: Option[StructType] = None,
                       override val encoding: Option[String] = Some("UTF-8")) extends DataFile[T]

case class JsonModel[T](path: String,
                        override val partition: Option[String] = None,
                        override val schema: Option[StructType] = None,
                        override val encoding: Option[String] = Some("UTF-8")) extends DataFile[T]

case class ParquetModel[T](path: String,
                           override val partition: Option[String] = None,
                           override val schema: Option[StructType] = None) extends DataFile[T]

case class XmlModel[T](path: String,
                       rowTag: Option[String] = None,
                       rootTag: Option[String] = None,
                       override val schema: Option[StructType] = None,
                       override val encoding: Option[String] = Some("UTF-8")) extends DataFile[T]