package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.CsvModel
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import scala.reflect.runtime.universe.TypeTag

import com.example.fw.infra.dataaccess.impl.ReaderMethodBuilder._
import com.example.fw.infra.dataaccess.impl.WriterMethodBuilder._

/**
 * CSVModelに対応したファイルアクセス機能を提供するクラス
 *
 * Sparkのcsvメソッドに対応
 *
 * @constructor コンストラクタ
 */
class CsvReaderWriter {

  /**
   * CSVファイルを読み込みDataFrameを返却する
   *
   * @param inputFile    入力ファイルのCsvModel
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(inputFile: CsvModel[Row], sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      //暗黙の型変換でメソッド拡張
      .buildOptionSep(inputFile)
      .buildOptionDateFormat(inputFile)
      .buildCsvSchema(inputFile)
      .buildOptionEncoding(inputFile)
      .buildOptionCompression(inputFile)
      .csv(inputFile.absolutePath)
  }

  /**
   * CSVファイルを読み込みDatasetを返却する
   *
   * @param inputFile    入力ファイルのCsvModel
   * @param sparkSession SparkSession
   * @tparam T CsvModelおよびDatasetの型パラメータ
   * @return Dataset
   */
  def readToDs[T <: Product : TypeTag](inputFile: CsvModel[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    sparkSession.read
      //暗黙の型変換でメソッド拡張
      .buildOptionSep(inputFile)
      .buildOptionDateFormat(inputFile)
      .buildCsvSchema(inputFile)
      .buildOptionEncoding(inputFile)
      .buildOptionCompression(inputFile)
      .csv(inputFile.absolutePath).as[T]
  }

  /**
   * 引数で受け取ったDataset/DataFrameを、指定のCSVファイルに出力する
   *
   * @param ds         出力対象のDataset/DataFrame
   * @param outputFile 出力先ファイルのCsvModel
   * @param saveMode   出力時のSaveMode
   * @tparam T CsvModelの型パラメータ
   */
  def writeFromDsDf[T](ds: Dataset[T], outputFile: CsvModel[T], saveMode: SaveMode): Unit = {
    ds.write.mode(saveMode)
      //暗黙の型変換でメソッド拡張
      .buildOptionSep(outputFile)
      .buildOptionHeader(outputFile)
      .buildOptionDateFormat(outputFile)
      .buildOptionCompression(outputFile)
      .buildOptionEncoding(outputFile)
      .buildPartitionBy(outputFile)
      .csv(outputFile.absolutePath)
  }

}
