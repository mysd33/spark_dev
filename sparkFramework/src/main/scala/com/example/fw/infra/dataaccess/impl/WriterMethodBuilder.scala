package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model._
import org.apache.spark.sql.DataFrameWriter

/**
 * 暗黙の型変換を用いてSparkのDataFrameWriterのメソッドを簡易に構築する
 * 糖衣構文を提供するimplicitクラスを定義するオブジェクト
 *
 * 利用する際は、以下のimport文を記述する。
 * {{{
 *   import com.example.fw.infra.dataaccess.impl.WriterMethodBuilder._
 * }}}
 *
 * @see [[http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter@csv(path:String):Unit]]
 * @see [[http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter@json(path:String):Unit]]
 * @see [[http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter@parquet(path:String):Unit]]
 */
object WriterMethodBuilder {


  implicit class RichDataFrameWriter[T](writer: DataFrameWriter[T]) {

    /**
     * sepを設定するoptionメソッドを構築する
     *
     * @param file CsvModel
     * @return DataFrameWriter
     */
    def buildOptionSep(file: CsvModel[T]): DataFrameWriter[T] = {
      file.sep match {
        case Some(sep) => writer.option("sep", sep)
        case _ => writer
      }
    }

    /**
     * dateFormat、timestampFormatを設定するoptionメソッドを構築する
     *
     * @param file HavingDateFormatトレイト
     * @return DataFrameWriter
     */
    def buildOptionDateFormat(file: HavingDateFormat): DataFrameWriter[T] = {
      val writer2 = file.dateFormat match {
        case Some(dateFormat) => writer.option("dateFormat", dateFormat)
        case _ => writer
      }
      file.timestampFormat match {
        case Some(timestampFormat) => writer2.option("timestampFormat", timestampFormat)
        case _ => writer2
      }
    }

    /**
     * headerを設定するoptionメソッドを構築する
     *
     * @param file
     * @return DataFrameWriter
     */
    def buildOptionHeader(file: CsvModel[T]): DataFrameWriter[T] = {
      writer.option("header", file.hasHeader)
    }

    /**
     * encodingを設定するoptionメソッドを構築する
     *
     * @param file TextFormatトレイト
     * @return DataFrameWriter
     */
    def buildOptionEncoding(file: TextFormat): DataFrameWriter[T] = {
      file.encoding match {
        case Some(encoding) => writer.option("encoding", encoding)
        case _ => writer
      }
    }

    /**
     * compressionを設定するoptionメソッドを構築する
     *
     * @param file Compressableトレイト
     * @return DataFrameWriter
     */
    def buildOptionCompression(file: Compressable): DataFrameWriter[T] = {
      file.compression match {
        case Some(compression) => writer.option("compression", compression)
        case _ => writer
      }
    }

    /**
     * partitionByメソッドを構築する
     *
     * @param file Partitionableトレイト
     * @return DataFrameWriter
     */
    def buildPartitionBy(file: Partitionable): DataFrameWriter[T] = {
      if (file.partitions.isEmpty) {
        writer
      } else {
        writer.partitionBy(file.partitions: _*)
      }
    }
  }

}
