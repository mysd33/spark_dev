package com.example.fw.domain.dataaccess

import com.example.fw.domain.model.DataFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * DataFileReaderWriterの実装トレイトのインタフェース。
 *
 * DataFileReaderWriterはDI機能を実装しており、インスタンス化する場合には、
 * DataFileReaderWriterImplトレイトの
 * 実装トレイトをミックスインすることで、Sparkのディストリビューション専用の
 * DataFileReaderWriter実装に切替えて使用する。
 *
 * @see [[com.example.fw.domain.dataaccess.DataFileReaderWriter]]
 */
trait DataFileReaderWriterImpl {
  /**
   * ファイルを読み込みRDDを返却する
   *
   * @param inputFile    入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @return RDD
   */
  def readToRDD(inputFile: DataFile[String], sparkSession: SparkSession): RDD[String]

  /**
   * ファイルを読み込みDatasetを返却する
   *
   * @param inputFile    入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @tparam T DataFileおよびDatasetの型パラメータ
   * @return Dataset
   */
  def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T]

  /**
   * ファイルを読み込みDataFrameを返却する
   *
   * @param inputFile    入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame


  /**
   * 引数で受け取ったRDDを指定のファイルに出力する
   *
   * @param rdd        出力対象のRDD
   * @param outputFile 出力先ファイルのDataFile
   * @tparam T RDDおよびDataFileの型パラメータ
   */
  def writeFromRDD[T](rdd: RDD[T], outputFile: DataFile[T]): Unit

  /**
   * 引数で受け取ったDataset/DataFrameを指定のファイルに出力する
   *
   * @param ds         出力対象のDataset/DataFrame
   * @param outputFile 出力先ファイルのDataFile
   * @param saveMode   出力時のSaveMode
   * @tparam T DataFileの型パラメータ
   */
  def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode = SaveMode.Overwrite): Unit
}
