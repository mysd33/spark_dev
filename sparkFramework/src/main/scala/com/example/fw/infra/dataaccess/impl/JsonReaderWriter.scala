package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.{DataFile, JsonModel}
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
   * @param inputFile    入力ファイルのJsonModel
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(inputFile: JsonModel[Row], sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .buildSchema(inputFile)
      .buildOptionDateFormat(inputFile)
      .buildOptionEncoding(inputFile)
      .buildOptionCompression(inputFile)
      .json(inputFile.absolutePath)
  }

  /**
   * Jsonファイルを読み込みDatasetを返却する
   *
   * @param inputFile    入力ファイルのJsonModel
   * @param sparkSession SparkSession
   * @tparam T JsonModelおよびDatasetの型パラメータ
   * @return Dataset
   */
  def readToDs[T <: Product : TypeTag](inputFile: JsonModel[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    sparkSession.read
      //暗黙の型変換でメソッド拡張
      .buildSchema(inputFile)
      .buildOptionDateFormat(inputFile)
      .buildOptionEncoding(inputFile)
      .buildOptionCompression(inputFile)
      .json(inputFile.absolutePath).as[T]
  }

  /**
   * 引数で受け取ったDataset/DataFrameを、指定のJsonファイルに出力する
   *
   * @param ds         出力対象のDataset/DataFrame
   * @param outputFile 出力先ファイルのJsonModel
   * @param saveMode   出力時のSaveMode
   * @tparam T JsonModelの型パラメータ
   */
  def writeFromDsDf[T](ds: Dataset[T], outputFile: JsonModel[T], saveMode: SaveMode): Unit = {
    ds.write.mode(saveMode)
      //暗黙の型変換でメソッド拡張
      .buildOptionDateFormat(outputFile)
      .buildOptionEncoding(outputFile)
      .buildOptionCompression(outputFile)
      .buildPartitionBy(outputFile)
      .json(outputFile.absolutePath)
  }
}
