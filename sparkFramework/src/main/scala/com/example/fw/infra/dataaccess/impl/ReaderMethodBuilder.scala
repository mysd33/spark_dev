package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model._
import org.apache.spark.sql.DataFrameReader

/**
 * 暗黙の型変換を用いてSparkのDataFrameReaderのメソッドを簡易に構築する
 * 糖衣構文を提供するimplicitクラスを定義するオブジェクト
 *
 * 利用する際は、以下のimport文を記述する。
 * {{{
 *   import com.example.fw.infra.dataaccess.impl.ReaderMethodBuilder._
 * }}}
 *
 * @see [[https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader@textFile(paths:String*):org.apache.spark.sql.Dataset[String]]]
 * @see [[http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader@csv(paths:String*):org.apache.spark.sql.DataFrame]]
 * @see [[http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader@json(paths:String*):org.apache.spark.sql.DataFrame]]
 * @see [[http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader@parquet(paths:String*):org.apache.spark.sql.DataFrame]]
 */
object ReaderMethodBuilder {

  implicit class RichDataFrameReader(reader: DataFrameReader) {
    /**
     * schemaメソッドを構築する
     *
     * @param file HavingSchemaトレイト
     * @return DataFrameReader
     */
    def buildSchema(file: HavingSchema): DataFrameReader = {
      file.schema match {
        case Some(schema) => reader.schema(schema)
        case _ => reader
      }
    }

    /**
     * CsvModelの場合のSchemaメソッドを構築する
     *
     * @param file CsvModel
     * @tparam T CsvModelの型パラメータ
     * @return DataFrameReader
     */
    def buildCsvSchema[T](file: CsvModel[T]): DataFrameReader = {
      file.schema match {
        case Some(schema) => {
          reader.schema(schema)
            .option("header", file.hasHeader)
        }
        case _ => {
          //CSVの場合、schema設定がない場合は、ヘッダをもとにスキーマ推定
          assert(file.hasHeader, "HavingSchema.hasHeader must be true")
          reader
            .option("header", file.hasHeader)
            .option("inferSchema", "true")
        }
      }
    }

    /**
     * sepを設定するoptionメソッドを構築する
     *
     * @param file CsvModel
     * @tparam T CsvModelの型パラメータ
     * @return DataFrameReader
     */
    def buildOptionSep[T](file: CsvModel[T]): DataFrameReader = {
      file.sep match {
        case Some(delimiter) => reader.option("sep", delimiter)
        case _ => reader
      }
    }

    /**
     * dateFormat、timestampFormatを設定するoptionメソッドを構築する
     *
     * @param file HavingDateFormatトレイト
     * @return DataFrameReader
     */
    def buildOptionDateFormat(file: HavingDateFormat): DataFrameReader = {
      val reader2 = file.dateFormat match {
        case Some(dateFormat) => reader.option("dateFormat", dateFormat)
        case _ => reader
      }
      file.timestampFormat match {
        case Some(timestampFormat) => reader2.option("timestampFormat", timestampFormat)
        case _ => reader2
      }
    }

    /**
     * encodingを設定するoptionメソッドを構築する
     *
     * @param file TextFormatトレイト
     * @return DataFrameReader
     */
    def buildOptionEncoding(file: TextFormat): DataFrameReader = {
      file.encoding match {
        case Some(encoding) => reader.option("encoding", encoding)
        case _ => reader
      }
    }

    /**
     * compressionを設定するoptionメソッドを構築する
     *
     * @param file Compressableトレイト
     * @return DataFrameReader
     */
    def buildOptionCompression(file: Compressable): DataFrameReader = {
      file.compression match {
        case Some(compression) => reader.option("compression", compression)
        case _ => reader
      }
    }

  }

}
