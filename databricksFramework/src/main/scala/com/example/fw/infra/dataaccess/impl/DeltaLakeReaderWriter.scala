package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.ParquetModel
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

import com.example.fw.infra.dataaccess.impl.WriterMethodBuilder._

/**
 * ParquetModelに対応したファイル・テーブルアクセス機能を提供するクラス
 *
 * Databricksで動作可能な、Paruquet拡張のDeltaLakeで扱えるようにしたもの。
 *
 * Sparkのformat("delta").load、format("delta").saveメソッドに対応
 *
 * @constructor コンストラクタ
 */
class DeltaLakeReaderWriter {
  private val formatName = "delta"

  /**
   * Parquetファイル（またはテーブル）を読み込みDataFrameを返却する
   *
   * @param inputFile    入力ファイルのParquetModel
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(inputFile: ParquetModel[Row], sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .format(formatName)
      .load(inputFile.absolutePath)
  }

  /**
   * Parquetファイル（またはテーブル）を読み込みDatasetを返却する
   *
   * @param inputFile    入力ファイルのParquetModel
   * @param sparkSession SparkSession
   * @tparam T ParquetModelおよびDatasetの型パラメータ
   * @return Dataset
   */
  def readToDs[T <: Product : TypeTag](inputFile: ParquetModel[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    sparkSession.read
      .format(formatName)
      .load(inputFile.absolutePath)
      .as[T]
  }

  /**
   * 引数で受け取ったDataset/DataFrameを、指定のParquetファイル（またはテーブル）に出力する
   *
   * @param ds         出力対象のDataset/DataFrame
   * @param outputFile 出力先ファイルのParquetModel
   * @param saveMode   出力時のSaveMode
   * @tparam T ParquetModelの型パラメータ
   */
  def writeFromDsDf[T](ds: Dataset[T], outputFile: ParquetModel[T], saveMode: SaveMode): Unit = {
    ds.write.mode(saveMode)
      //暗黙の型変換でメソッド拡張
      .buildOptionCompression(outputFile)
      .buildPartitionBy(outputFile)
      .format(formatName).save(outputFile.absolutePath)
  }
}
