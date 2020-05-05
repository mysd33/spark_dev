package com.example.fw.domain.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.reflect.runtime.universe.TypeTag

/**
 * 入出力をDatasetで扱うためのLogicの基底クラス。
 *
 * Datasetが型パラメータを指定する必要があるため、
 * 1つの入力Datasetに対して1つのDatasetを返却する場合のみ利用できる。
 *
 * 業務開発者は本クラスを継承してLogicクラスを実装する。
 *
 * @constructor コンストラクタ
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 * @tparam T 入力ファイルつまり、inputFileのDataFile、Datasetが扱う型パラメータ
 * @tparam U 出力ファイルつまり、outputFileのDataFile、Datasetが扱う型パラメータ
 */
abstract class DatasetBLogic1to1[T <: Product : TypeTag, U <: Product : TypeTag]
(val dataFileReaderWriter: DataFileReaderWriter) extends Logic {
  /** 入力ファイルのDataFileを実装する。 1ファイルのみ指定できる。 */
  val inputFile: DataFile[T]
  /** 出力ファイルのDataFileを実装する。 1ファイルのみ指定できる。 */
  val outputFile: DataFile[U]

  /**
   * @see [[com.example.fw.domain.logic.Logic#execute(org.apache.spark.sql.SparkSession)]]
   * @param sparkSession SparkSession
   */
  override final def execute(sparkSession: SparkSession): Unit = {
    try {
      setUp(sparkSession)
      val inputDataset = input(sparkSession)
      val outputDataset = process(inputDataset, sparkSession)
      output(outputDataset)
    } finally {
      tearDown(sparkSession)
    }
  }

  /**
   * processメソッド前の処理を実行する。デフォルト実装は、ビジネスロジックを開始するログを出力する。
   *
   * Logicクラス個別に実施したい処理があればはoverrideする。
   * {{{
   *   override def setUp(sparkSession: SparkSession): Unit = {
   *       //implement
   *
   *       super.setUp(sparkSession)
   *   }
   * }}}
   *
   * @param sparkSession SparkSession
   */
  def setUp(sparkSession: SparkSession): Unit = {
    logInfo("ビジネスロジック開始:" + getClass().getTypeName())
  }

  final def input(sparkSession: SparkSession): Dataset[T] = {
    dataFileReaderWriter.readToDs(inputFile, sparkSession)
  }

  def process(ds: Dataset[T], sparkSession: SparkSession): Dataset[U]

  final def output(ds: Dataset[U]): Unit = {
    dataFileReaderWriter.writeFromDs(ds, outputFile)
  }

  /**
   * processメソッド後の処理を実行する。デフォルト実装は、ビジネスロジックを終了するログを出力する。
   *
   * Logicクラス個別に実施したい処理があればはoverrideする。
   * {{{
   *   override def tearDown(sparkSession: SparkSession): Unit = {
   *       //implement
   *
   *       super.tearDown(sparkSession)
   *   }
   * }}}
   *
   * @param sparkSession SparkSession
   */
  def tearDown(sparkSession: SparkSession): Unit = {
    logInfo("ビジネスロジック終了")
  }
}
