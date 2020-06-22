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
     * @param model CsvModel
     * @return DataFrameWriter
     */
    def buildOptionSep(model: CsvModel[T]): DataFrameWriter[T] = {
      model.sep match {
        case Some(sep) => writer.option("sep", sep)
        case _ => writer
      }
    }

    /**
     * dateFormat、timestampFormatを設定するoptionメソッドを構築する
     *
     * @param model HavingDateFormatトレイト
     * @return DataFrameWriter
     */
    def buildOptionDateFormat(model: HavingDateFormat): DataFrameWriter[T] = {
      val writer2 = model.dateFormat match {
        case Some(dateFormat) => writer.option("dateFormat", dateFormat)
        case _ => writer
      }
      model.timestampFormat match {
        case Some(timestampFormat) => writer2.option("timestampFormat", timestampFormat)
        case _ => writer2
      }
    }

    /**
     * headerを設定するoptionメソッドを構築する
     *
     * @param model
     * @return DataFrameWriter
     */
    def buildOptionHeader(model: CsvModel[T]): DataFrameWriter[T] = {
      writer.option("header", model.hasHeader)
    }

    /**
     * encodingを設定するoptionメソッドを構築する
     *
     * @param model TextFormatトレイト
     * @return DataFrameWriter
     */
    def buildOptionEncoding(model: TextFormat): DataFrameWriter[T] = {
      model.encoding match {
        case Some(encoding) => writer.option("encoding", encoding)
        case _ => writer
      }
    }

    /**
     * compressionを設定するoptionメソッドを構築する
     *
     * @param model Compressableトレイト
     * @return DataFrameWriter
     */
    def buildOptionCompression(model: Compressable): DataFrameWriter[T] = {
      model.compression match {
        case Some(compression) => writer.option("compression", compression)
        case _ => writer
      }
    }

    /**
     * partitionByメソッドを構築する
     *
     * @param model Partitionableトレイト
     * @return DataFrameWriter
     */
    def buildPartitionBy(model: Partitionable): DataFrameWriter[T] = {
      if (model.partitions.isEmpty) {
        writer
      } else {
        writer.partitionBy(model.partitions: _*)
      }
    }
  }

}
