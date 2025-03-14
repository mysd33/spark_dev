package com.example.fw.infra.dataaccess.impl

import com.databricks.spark.xml._
import com.example.fw.domain.model.XmlModel
import org.apache.spark.sql._

import com.example.fw.infra.dataaccess.impl.ReaderMethodBuilder._
import com.example.fw.infra.dataaccess.impl.WriterMethodBuilder._

//TODO: spark-xmlは、ネストした複雑なXMLデータ構造だと煩雑なコードになってしまうし、ファイルを読んで逐次動作させながらでないと実装が難しいので使わない
/**
 * XmlModelに対応したファイルアクセス機能
 *
 * spark-xmlを利用しており、xmlメソッドに対応する
 *
 * spark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しないので注意
 *
 * @see [[https://github.com/databricks/spark-xml/blob/master/README.md]]
 * @deprecated spark-xmlは、ネストした複雑なXMLデータ構造だと煩雑なコードになってしまうし、ファイルを読んで逐次動作させながらでないと実装が難しいので使わない
 */
class XmlReaderWriter {
  //Optionの実装
  //https://github.com/databricks/spark-xml#features

  /**
   * Xmlファイルを読み込みDataFrameを返却する
   *
   * @param input        入力のXmlModel
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(input: XmlModel[Row], sparkSession: SparkSession): DataFrame = {
    val reader = input.rowTag match {
      case Some(rowTag) => sparkSession.read.option("rowTag", rowTag)
      case _ => sparkSession.read
    }
    val reader2 = input.encoding match {
      //spark-xmlではencodingではなくcharset
      case Some(encoding) => reader.option("charset", encoding)
      case _ => reader
    }
    reader2
      //暗黙の型変換でメソッド拡張
      .buildSchema(input)
      .xml(input.absolutePath)
  }

  /**
   * 引数で受け取ったDataset/DataFrameを、指定のXmlファイルに出力する
   *
   * @param ds       出力対象のDataset/DataFrame
   * @param output   出力先のXmlModel
   * @param saveMode 出力時のSaveMode
   * @tparam T CsvModelの型パラメータ
   */
  def writeFromDsDf[T](ds: Dataset[T], output: XmlModel[T], saveMode: SaveMode): Unit = {
    val writer = ds.write.mode(saveMode)
    val writer2 = output.rootTag match {
      case Some(rootTag) => writer.option("rootTag", rootTag)
      case _ => writer
    }
    val writer3 = output.rowTag match {
      case Some(rowTag) => writer2.option("rowTag", rowTag)
      case _ => writer2
    }
    writer3
      //暗黙の型変換でメソッド拡張
      .buildOptionCompression(output)
      .xml(output.absolutePath)
  }

}
