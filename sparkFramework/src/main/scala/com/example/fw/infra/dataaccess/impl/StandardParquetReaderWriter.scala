package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.ParquetModel
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

import com.example.fw.infra.dataaccess.impl.WriterMethodBuilder._

/**
 * ParquetModelに対応したファイル・テーブルアクセス機能を提供するクラス
 *
 * Sparkのparquetメソッドに対応
 *
 * @constructor コンストラクタ
 */
class StandardParquetReaderWriter {
  /**
   * Parquetファイル（またはテーブル）を読み込みDataFrameを返却する
   *
   * @param input        入力のParquetModel
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(input: ParquetModel[Row], sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .parquet(input.absolutePath)
  }

  /**
   * Parquetファイル（またはテーブル）を読み込みDatasetを返却する
   *
   * @param input        入力のParquetModel
   * @param sparkSession SparkSession
   * @tparam T ParquetModelおよびDatasetの型パラメータ
   * @return Dataset
   */
  def readToDs[T <: Product : TypeTag](input: ParquetModel[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    sparkSession.read
      .parquet(input.absolutePath)
      .as[T]
  }

  /**
   * 引数で受け取ったDataset/DataFrameを、指定のParquetファイル（またはテーブル）に出力する
   *
   * @param ds       出力対象のDataset/DataFrame
   * @param output   出力先ファイルのParquetModel
   * @param saveMode 出力時のSaveMode
   * @tparam T ParquetModelの型パラメータ
   */
  def writeFromDsDf[T](ds: Dataset[T], output: ParquetModel[T], saveMode: SaveMode): Unit = {
    ds.write.mode(saveMode)
      //暗黙の型変換でメソッド拡張
      .buildOptionCompression(output)
      .buildPartitionBy(output)
      .parquet(output.absolutePath)
  }
}
