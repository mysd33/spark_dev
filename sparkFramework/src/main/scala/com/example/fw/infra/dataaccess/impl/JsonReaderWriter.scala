package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.{DataModel, JsonModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag
import com.example.fw.infra.dataaccess.impl.ReaderMethodBuilder._
import com.example.fw.infra.dataaccess.impl.WriterMethodBuilder._

/**
 * JsonModelに対応したファイルアクセス機能を提供するクラス
 *
 * Sparkのjsonメソッドに対応
 *
 * @constructor コンストラクタ
 */
class JsonReaderWriter {
  /**
   * Jsonファイルを読み込みDataFrameを返却する
   *
   * @param input        入力のJsonModel
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(input: JsonModel[Row], sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .buildSchema(input)
      .buildOptionDateFormat(input)
      .buildOptionEncoding(input)
      .buildOptionCompression(input)
      .json(input.absolutePath)
  }

  /**
   * Jsonファイルを読み込みDatasetを返却する
   *
   * @param input        入力のJsonModel
   * @param sparkSession SparkSession
   * @tparam T JsonModelおよびDatasetの型パラメータ
   * @return Dataset
   */
  def readToDs[T <: Product : TypeTag](input: JsonModel[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    sparkSession.read
      //暗黙の型変換でメソッド拡張
      .buildSchema(input)
      .buildOptionDateFormat(input)
      .buildOptionEncoding(input)
      .buildOptionCompression(input)
      .json(input.absolutePath).as[T]
  }

  /**
   * 引数で受け取ったDataset/DataFrameを、指定のJsonファイルに出力する
   *
   * @param ds       出力対象のDataset/DataFrame
   * @param output   出力先のJsonModel
   * @param saveMode 出力時のSaveMode
   * @tparam T JsonModelの型パラメータ
   */
  def writeFromDsDf[T](ds: Dataset[T], output: JsonModel[T], saveMode: SaveMode): Unit = {
    ds.write.mode(saveMode)
      //暗黙の型変換でメソッド拡張
      .buildOptionDateFormat(output)
      .buildOptionEncoding(output)
      .buildOptionCompression(output)
      .buildPartitionBy(output)
      .json(output.absolutePath)
  }
}
