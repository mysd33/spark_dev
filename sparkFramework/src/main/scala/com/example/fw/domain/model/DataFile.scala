package com.example.fw.domain.model

import com.example.fw.domain.const.FWConst._
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.sql.types.StructType

/**
 * Spark APで扱うファイルを表す最上位のトレイト
 *
 * ファイルのパスやデータセットが扱うデータ形式を保持する
 *
 * @tparam T データセットが扱うデータ型。RDDやDataFrame、Dataset等で扱う型パラメータと対応する。
 */
sealed trait DataFile[+T] extends Serializable {
  require(relativePath != null)
  /** ファイルの相対パス */
  val relativePath: String
  /** ファイルのフルパス
   *
   * application-xxx.propertiesに指定したプロパティファイルの"basepath"の値と
   * relativePathの値を連結し、実際のファイルパスを指定できる。
   * これにより、APを変更せずに、動作環境によって物理的なファイルパスを切替え可能となる。
   */
  final val absolutePath: String = {
    //プロパティでベースパスの置き換え
    val basePath = ResourceBundleManager.get(BASE_PATH_KEY)
    basePath + relativePath
  }
}

/**
 * ファイルがパーティショニング（partitionBy）可能であることを示すトレイト
 */
trait Partitionable {
  /** パーティション列
   *
   * SparkのpartitionByメソッドと対応
   */
  val partitions: Seq[String]
}

/**
 * ファイルがテキスト形式であることを示すトレイト
 */
trait TextFormat {
  /** ファイルのエンコーディング
   *
   * Sparkのoptionメソッドの"encoding"と対応。UTF-8、MS932等を指定する。
   * */
  val encoding: Option[String]
}

/**
 * ファイルが圧縮可能であることを示すトレイト
 */
trait Compressable {
  /**
   * 圧縮形式
   *
   * Sparkのoptionメソッドの"compression"と対応。bzip2、snappy等を指定する。
   */
  val compression: Option[String]
}

/**
 * ファイルがスキーマ情報を持つことを示すトレイト
 */
trait HavingSchema {
  /**
   * スキーマ情報
   *
   * Sparkのschemaメソッドと対応。
   */
  val schema: Option[StructType]
}

/**
 * ファイルが日付フォーマットを持つことを示すトレイト
 */
trait HavingDateFormat {
  /**
   * 日付文字列のフォーマット
   *
   * Sparkのoptionメソッドの"dateFormat"と対応。
   * SimpleDataFormatクラスで使用できるフォーマットを指定する。
   */
  val dateFormat: Option[String]

  /**
   * タイムスタンプ文字列のフォーマット
   *
   * Sparkのoptionメソッドの"timestampFormat"と対応。
   * SimpleDataFormatクラスで使用できるフォーマットを指定する。
   */
  val timestampFormat: Option[String]
}

/**
 * 各行をテキストとして扱うファイルのModel
 *
 * @param relativePath ファイルの相対パス
 * @param encoding     @see [[com.example.fw.domain.model.TextFormat]]
 * @tparam T データセットが扱うデータ型。RDDやDataFrame、Dataset等で扱う型パラメータと対応する。
 */
case class TextLineModel[T](override val relativePath: String,
                            override val encoding: Option[String] = None)
  extends DataFile[T] with TextFormat

/**
 * マルチフォーマットCSVファイルのModel
 *
 * @param relativePath    ファイルの相対パス
 * @param recordDelimiter 処理単位のレコードの区切り文字列。デフォルトは、NUL（\\u0000）
 *                        Hadoopの"textinputformat.record.delimiter"に対応する。
 * @param encoding        @see [[com.example.fw.domain.model.TextFormat]]
 * @tparam T データセットが扱うデータ型。RDDやDataFrame、Dataset等で扱う型パラメータと対応する。
 */
case class MultiFormatCsvModel[T](override val relativePath: String,
                                  recordDelimiter: Option[String] = Some(DEFAULT_MULTIFORMAT_CSV_DELIMITER),
                                  override val encoding: Option[String] = None)
  extends DataFile[T] with TextFormat

/**
 * CSVファイルのModel
 *
 * @param relativePath    ファイルの相対パス
 * @param hasHeader       先頭行にヘッダ行を持つか表す。Sparkのoptionメソッドの"header"と対応。
 * @param sep             １行の区切り文字列。Sparkのoptionメソッドの"sep"と対応。
 * @param partitions       @see [[com.example.fw.domain.model.Partitionable.partitions]]
 * @param schema          @see [[com.example.fw.domain.model.HavingSchema.schema]]
 * @param encoding        @see [[com.example.fw.domain.model.TextFormat.encoding]]
 * @param dateFormat      @see [[com.example.fw.domain.model.HavingDateFormat.dateFormat]]
 * @param timestampFormat @see [[com.example.fw.domain.model.HavingDateFormat.timestampFormat]]
 * @param compression     @see [[com.example.fw.domain.model.Compressable.compression]]
 * @tparam T データセットが扱うデータ型。RDDやDataFrame、Dataset等で扱う型パラメータと対応する。
 */
case class CsvModel[T](override val relativePath: String,
                       hasHeader: Boolean = false,
                       sep: Option[String] = None,
                       override val partitions: Seq[String] = Nil,
                       override val schema: Option[StructType] = None,
                       override val encoding: Option[String] = None,
                       override val dateFormat: Option[String] = None,
                       override val timestampFormat: Option[String] = None,
                       //TODO:emptyValue,nullValue,nanValueのCSVでの扱い
                       override val compression: Option[String] = None)
  extends DataFile[T] with TextFormat with Partitionable with HavingSchema with HavingDateFormat with Compressable

/**
 * JSONファイルのModel
 *
 * @param relativePath    ファイルの相対パス
 * @param partitions       @see [[com.example.fw.domain.model.Partitionable.partitions]]
 * @param schema          @see [[com.example.fw.domain.model.HavingSchema.schema]]
 * @param encoding        @see [[com.example.fw.domain.model.TextFormat.encoding]]
 * @param dateFormat      @see [[com.example.fw.domain.model.HavingDateFormat.dateFormat]]
 * @param timestampFormat @see [[com.example.fw.domain.model.HavingDateFormat.timestampFormat]]
 * @param compression     @see [[com.example.fw.domain.model.Compressable.compression]]
 * @tparam T データセットが扱うデータ型。RDDやDataFrame、Dataset等で扱う型パラメータと対応する。
 */
case class JsonModel[T](override val relativePath: String,
                        override val partitions: Seq[String] = Nil,
                        override val schema: Option[StructType] = None,
                        override val encoding: Option[String] = None,
                        override val dateFormat: Option[String] = None,
                        override val timestampFormat: Option[String] = None,
                        override val compression: Option[String] = None)
  extends DataFile[T] with TextFormat with Partitionable with HavingSchema with HavingDateFormat with Compressable

/**
 * Parquet（またはParquet拡張のDeltaLake）ファイルのModel
 *
 * @param relativePath ファイルの相対パス
 * @param partitions    @see [[com.example.fw.domain.model.Partitionable.partitions]]
 * @param compression  @see [[com.example.fw.domain.model.Compressable.compression]]
 * @tparam T データセットが扱うデータ型。RDDやDataFrame、Dataset等で扱う型パラメータと対応する。
 */
case class ParquetModel[T](override val relativePath: String,
                           override val partitions: Seq[String] = Nil,
                           override val compression: Option[String] = None)
  extends DataFile[T] with Partitionable with Compressable

//TODO: spark-xmlは、ネストした複雑なXMLデータ構造だと煩雑なコードになってしまうし、ファイルを読んで逐次動作させながらでないと実装が難しいので使わない
/**
 * XMLファイルのModel
 *
 * spark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しないので注意
 *
 * @see [[https://github.com/databricks/spark-xml/blob/master/README.md]]
 * @param relativePath ファイルの相対パス
 * @param rowTag       spark-xmlのoptionメソッドの"rowTag"と対応
 * @param rootTag      spark-xmlのoptionメソッドの"rootTag"と対応
 * @param schema       @see [[com.example.fw.domain.model.HavingSchema.schema]]
 * @param encoding     @see [[com.example.fw.domain.model.TextFormat.encoding]]
 * @param compression  @see [[com.example.fw.domain.model.Compressable.compression]]
 * @deprecated spark-xmlは、ネストした複雑なXMLデータ構造だと煩雑なコードになってしまうし、ファイルを読んで逐次動作させながらでないと実装が難しいので使わない
 * @tparam T データセットが扱うデータ型。RDDやDataFrame、Dataset等で扱う型パラメータと対応する。
 */
case class XmlModel[T](override val relativePath: String,
                       rowTag: Option[String] = None,
                       rootTag: Option[String] = None,
                       override val schema: Option[StructType] = None,
                       override val encoding: Option[String] = None,
                       override val compression: Option[String] = None)
  extends DataFile[T] with TextFormat with HavingSchema with Compressable


/**
 * データウェアハウス(データマート)基盤のDBを扱うModel
 *
 * Azureの場合、通常、Synapse Analytics(SQL Data Warehouse)に接続することを想定しているが
 * 業務ロジックを変更せずに、Snowflake等の他のデータウェアハウス基盤でも利用できるよう、
 * 将来的に特定のクラウドや製品に依存せずにReaderWriterの実装を切り替え可能な作りとしている。
 *
 * @param dbTable 読み書きするDBのテーブル名。queryとは同時に指定できない。書き込みの時はdbTableを必ず指定する。
 * @param query   DBを参照する場合のクエリ。dbTableと同時に指定できない。
 * @tparam T データセットが扱うデータ型。RDDやDataFrame、Dataset等で扱う型パラメータと対応する。
 */
case class DwDmModel[T](dbTable: Option[String] = None,
                        query: Option[String] = None,
                        //TODO: DBのためファイルパスがないので暫定でダミー値を格納。
                        //TODO: 本来はDataFileより一つ上の概念を表すトレイトを追加し全体のリファクタリング要
                        override val relativePath: String = ???
                       ) extends DataFile[T] {
  //いずれか一方が定義されていること
  assert(!(dbTable.isDefined && query.isDefined))
  assert(!(dbTable.isEmpty && query.isEmpty))
}