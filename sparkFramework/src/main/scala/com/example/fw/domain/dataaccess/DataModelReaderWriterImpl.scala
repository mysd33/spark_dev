package com.example.fw.domain.dataaccess

import com.example.fw.domain.model.DataModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * DataModelReaderWriterの実装トレイトのインタフェース。
 *
 * DataModelReaderWriterはDI機能を実装しており、インスタンス化する場合には、
 * DataModelReaderWriterImplトレイトの
 * 実装トレイトをミックスインすることで、Sparkのディストリビューション専用の
 * DataModelReaderWriter実装に切替えて使用する。
 *入力ファイルのDataModel
 * @see [[com.example.fw.domain.dataaccess.DataModelReaderWriter]]
 */
trait DataModelReaderWriterImpl {
  /**
   * ファイルを読み込みRDDを返却する
   *
   * @param input    入力ファイルのDataModel
   * @param sparkSession SparkSession
   * @return RDD
   */
  def readToRDDImpl(input: DataModel[String], sparkSession: SparkSession): RDD[String]

  /**
   * ファイルを読み込みDatasetを返却する
   *
   * @param input    入力ファイルのDataModel
   * @param sparkSession SparkSession
   * @tparam T DataFileおよびDatasetの型パラメータ
   * @return Dataset
   */
  def readToDsImpl[T <: Product : TypeTag](input: DataModel[T], sparkSession: SparkSession): Dataset[T]


  /**
   * ファイルを読み込みDatasetを返却する
   *
   * @param input    入力ファイルのDataModel
   * @param sparkSession SparkSession
   * @return Dataset
   */
  def readToDsImpl(input: DataModel[String], sparkSession: SparkSession): Dataset[String]

  /**
   * ファイルを読み込みDataFrameを返却する
   *
   * @param input    入力ファイルのDataModel
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDfImpl(input: DataModel[Row], sparkSession: SparkSession): DataFrame


  /**
   * 引数で受け取ったRDDを指定のファイルに出力する
   *
   * @param rdd        出力対象のRDD
   * @param output 出力先ファイルのDataModel
   * @tparam T RDDおよびDataFileの型パラメータ
   */
  def writeFromRDDImpl[T](rdd: RDD[T], output: DataModel[T]): Unit

  /**
   * 引数で受け取ったDataset/DataFrameを指定のファイルに出力する
   *
   * @param ds         出力対象のDataset/DataFrame
   * @param output 出力先ファイルのDataModel
   * @param saveMode   出力時のSaveMode
   * @tparam T DataFileの型パラメータ
   */
  def writeFromDsDfImpl[T](ds: Dataset[T], output: DataModel[T], saveMode: SaveMode = SaveMode.Overwrite): Unit
}
