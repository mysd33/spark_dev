package com.example.fw.domain.dataaccess

import com.example.fw.domain.model.DataModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * ファイル・テーブルアクセス機能のインタフェース。
 * DataModelを元にファイルやテーブルの読み書きを行う。
 *
 * DI機能を実装しており、インスタンス化する場合には、
 * 自分型アノテーションでDataModelReaderWriterImplトレイトの
 * 実装トレイトをミックスインすることで、Sparkのディストリビューション専用の
 * DataModelReaderWriter実装に切替えて使用する。
 * {{{
 *   //example for Standard Spark Application
 *   new DataModelReaderWriter with StandardSparkDataModelReaderWriter
 *
 *   //example for Databricks(and Delta Lake) Spark Application
 *   new DataModelReaderWriter with DatabricksDataModelReaderWriter
 * }}}
 *
 * @constructor コンストラクタ
 */
class DataModelReaderWriter {
  //自分型アノテーションでDataFileReaderWriterの実装をDI
  self: DataModelReaderWriterImpl =>
  /**
   * ファイルを読み込みRDDを返却する
   * @param input 入力のDataModel
   * @param sparkSession SparkSession
   * @return RDD
   */
  def readToRDD(input: DataModel[String], sparkSession: SparkSession): RDD[String] = {
    self.readToRDDImpl(input, sparkSession)
  }

  /**
   * ファイルを読み込みDatasetを返却する
   * @param input 入力のDataModel
   * @param sparkSession SparkSession
   * @tparam T DataFileおよびDatasetの型パラメータ
   * @return Dataset
   */
  def readToDs[T <: Product : TypeTag](input: DataModel[T], sparkSession: SparkSession): Dataset[T] = {
    self.readToDsImpl(input, sparkSession)
  }

  /**
   * ファイルを読み込みDataFrameを返却する
   * @param input 入力のDataModel
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(input: DataModel[Row], sparkSession: SparkSession): DataFrame = {
    self.readToDfImpl(input, sparkSession)
  }

  /**
   * 引数で受け取ったRDDを、指定のファイルに出力する
   * @param rdd 出力対象のRDD
   * @param output 出力のDataModel
   * @tparam T RDDおよびDataFileの型パラメータ
   */
  def writeFromRDD[T](rdd: RDD[T], output: DataModel[T]): Unit = {
    self.writeFromRDDImpl(rdd, output)
  }

  /**
   * 引数で受け取ったDataFrameを、指定のファイルに出力する
   * @param df 出力対象のDataFrame
   * @param output 出力のDataModel
   * @param saveMode 出力時のSaveMode
   * @tparam T DataFileの型パラメータ
   */
  def writeFromDf[T](df: DataFrame, output: DataModel[Row], saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    self.writeFromDsDfImpl(df, output, saveMode)
  }

  /**
   * 引数で受け取ったDatasetを、指定のファイルに出力する
   * @param ds 出力対象のDataset
   * @param output 出力のDataModel
   * @param saveMode 出力時のSaveMode
   * @tparam T DatasetおよびDataFileの型パラメータ
   */
  def writeFromDs[T <: Product : TypeTag](ds: Dataset[T], output: DataModel[T], saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    self.writeFromDsDfImpl(ds, output, saveMode)
  }
}
