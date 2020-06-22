package com.example.fw.domain.dataaccess

import com.example.fw.domain.model.DataModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * ファイル・テーブルアクセス機能のインタフェース。
 * DataFileを元にファイルやテーブルの読み書きを行う。
 *
 * DI機能を実装しており、インスタンス化する場合には、
 * 自分型アノテーションでDataFileReaderWriterImplトレイトの
 * 実装トレイトをミックスインすることで、Sparkのディストリビューション専用の
 * DataFileReaderWriter実装に切替えて使用する。
 * {{{
 *   //example for Standard Spark Application
 *   new DataFileReaderWriter with StandardSparkDataFileReaderWriter
 *
 *   //example for Databricks(and Delta Lake) Spark Application
 *   new DataFileReaderWriter with DatabricksDataFileReaderWriter
 * }}}
 *
 * @constructor コンストラクタ
 */
class DataFileReaderWriter {
  //自分型アノテーションでDataFileReaderWriterの実装をDI
  self: DataFileReaderWriterImpl =>
  /**
   * ファイルを読み込みRDDを返却する
   * @param inputFile 入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @return RDD
   */
  def readToRDD(inputFile: DataModel[String], sparkSession: SparkSession): RDD[String] = {
    self.readToRDDImpl(inputFile, sparkSession)
  }

  /**
   * ファイルを読み込みDatasetを返却する
   * @param inputFile 入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @tparam T DataFileおよびDatasetの型パラメータ
   * @return Dataset
   */
  def readToDs[T <: Product : TypeTag](inputFile: DataModel[T], sparkSession: SparkSession): Dataset[T] = {
    self.readToDsImpl(inputFile, sparkSession)
  }

  /**
   * ファイルを読み込みDataFrameを返却する
   * @param inputFile 入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(inputFile: DataModel[Row], sparkSession: SparkSession): DataFrame = {
    self.readToDfImpl(inputFile, sparkSession)
  }

  /**
   * 引数で受け取ったRDDを、指定のファイルに出力する
   * @param rdd 出力対象のRDD
   * @param outputFile 出力先ファイルのDataFile
   * @tparam T RDDおよびDataFileの型パラメータ
   */
  def writeFromRDD[T](rdd: RDD[T], outputFile: DataModel[T]): Unit = {
    self.writeFromRDDImpl(rdd, outputFile)
  }

  /**
   * 引数で受け取ったDataFrameを、指定のファイルに出力する
   * @param df 出力対象のDataFrame
   * @param outputFile 出力先ファイルのDataFile
   * @param saveMode 出力時のSaveMode
   * @tparam T DataFileの型パラメータ
   */
  def writeFromDf[T](df: DataFrame, outputFile: DataModel[Row], saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    self.writeFromDsDfImpl(df, outputFile, saveMode)
  }

  /**
   * 引数で受け取ったDatasetを、指定のファイルに出力する
   * @param ds 出力対象のDataset
   * @param outputFile 出力先ファイルのDataFile
   * @param saveMode 出力時のSaveMode
   * @tparam T DatasetおよびDataFileの型パラメータ
   */
  def writeFromDs[T <: Product : TypeTag](ds: Dataset[T], outputFile: DataModel[T], saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    self.writeFromDsDfImpl(ds, outputFile, saveMode)
  }
}
