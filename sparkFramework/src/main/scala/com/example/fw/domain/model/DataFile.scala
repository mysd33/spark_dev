package com.example.fw.domain.model

import com.example.fw.domain.const.FWConst._
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.sql.types.StructType


sealed trait DataFile[+T] extends Serializable {
  require(relativePath != null)
  val relativePath: String
  val absolutePath: String = {
    //プロパティでベースパスの置き換え
    val basePath = ResourceBundleManager.get(BASE_PATH_KEY)
    basePath + relativePath
  }
}

trait Partitionable {
  val partition: Option[String]
}

trait TextFormat {
  val encoding: Option[String]
}

trait Compressable {
  val compression: Option[String]
}

trait HavingSchema {
  val schema: Option[StructType]
}

trait HavingDateFormat {
  val dateFormat: Option[String]
  val timestampFormat: Option[String]
}

trait Splittable {
  val delimiter: Option[String]
}

case class TextLineModel[T](override val relativePath: String,
                            override val encoding: Option[String] = None)
  extends DataFile[T] with TextFormat

case class MultiFormatCsvModel[T](override val relativePath: String,
                                  override val delimiter: Option[String] = Some(DEFAULT_MULTIFORMAT_CSV_DELIMITER),
                                  override val encoding: Option[String] = None)
  extends DataFile[T] with TextFormat with Splittable

case class CsvModel[T](override val relativePath: String,
                       hasHeader: Boolean = false,
                       override val delimiter: Option[String] = None,
                       override val partition: Option[String] = None,
                       override val schema: Option[StructType] = None,
                       override val encoding: Option[String] = None,
                       override val dateFormat: Option[String] = None,
                       override val timestampFormat: Option[String] = None,
                       override val compression: Option[String] = None)
  extends DataFile[T] with TextFormat with Splittable with Partitionable with HavingSchema with HavingDateFormat with Compressable

case class JsonModel[T](override val relativePath: String,
                        override val partition: Option[String] = None,
                        override val schema: Option[StructType] = None,
                        override val encoding: Option[String] = None,
                        override val dateFormat: Option[String] = None,
                        override val timestampFormat: Option[String] = None,
                        override val compression: Option[String] = None)
  extends DataFile[T] with TextFormat with Partitionable with HavingSchema with HavingDateFormat with Compressable

case class ParquetModel[T](override val relativePath: String,
                           override val partition: Option[String] = None,
                           override val compression: Option[String] = None)
  extends DataFile[T] with Partitionable with Compressable

//TODO: spark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しないので本番開発では使用しない
case class XmlModel[T](override val relativePath: String,
                       rowTag: Option[String] = None,
                       rootTag: Option[String] = None,
                       override val schema: Option[StructType] = None,
                       override val encoding: Option[String] = None,
                       override val compression: Option[String] = None)
  extends DataFile[T] with TextFormat with HavingSchema with Compressable