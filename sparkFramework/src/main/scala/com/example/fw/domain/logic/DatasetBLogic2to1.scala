package com.example.fw.domain.logic

import com.example.fw.domain.const.FWMsgConst
import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.message.Message
import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * 入出力をDatasetで扱うためのLogicの基底クラス。
 *
 * 業務開発者は本クラスを継承してLogicクラスを実装する。
 *
 * Datasetを使用できるのでタイプセーフに実装できるが
 * 型パラメータを指定する必要があるため、
 * 2つの入力Datasetに対して1つのDatasetを返却する場合のみ利用できる。
 *
 * @constructor コンストラクタ
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 * @param args                 AP起動時の引数
 * @tparam T1 入力ファイルつまり、inputFile1のDataFile、Datasetが扱う型パラメータ
 * @tparam T2 入力ファイルつまり、inputFile2のDataFile、Datasetが扱う型パラメータ
 * @tparam U  出力ファイルつまり、outputFileのDataFile、Datasetが扱う型パラメータ
 */
abstract class DatasetBLogic2to1[T1 <: Product : TypeTag, T2 <: Product : TypeTag, U <: Product : TypeTag]
(val dataFileReaderWriter: DataFileReaderWriter, val args: Array[String] = null) extends Logic {
  require(dataFileReaderWriter != null)
  /** 1つ目の入力ファイルのDataFileを実装する。 */
  val inputFile1: DataFile[T1]
  /** 2つ目の入力ファイルのDataFileを実装する。 */
  val inputFile2: DataFile[T2]
  /** 出力ファイルのDataFileを実装する。 1ファイルのみ指定できる。 */
  val outputFile: DataFile[U]

  /**
   * @see [[com.example.fw.domain.logic.Logic.execute]]
   * @param sparkSession SparkSession
   */
  override final def execute(sparkSession: SparkSession): Unit = {
    try {
      setUp(sparkSession)
      val inputDatasets = input(sparkSession)
      val outputDatasets = process(inputDatasets._1, inputDatasets._2, sparkSession)
      output(outputDatasets)
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
    logInfo(Message.get(FWMsgConst.I_FW_001, getClass().getTypeName()))
  }

  /**
   * inputFile1、inputFile2のDataFileからDatasetを取得する
   *
   * @param sparkSession SparkSession
   * @return Datasetのタプル
   */
  final def input(sparkSession: SparkSession): (Dataset[T1], Dataset[T2]) = {
    val input1 = dataFileReaderWriter.readToDs(inputFile1, sparkSession)
    val input2 = dataFileReaderWriter.readToDs(inputFile2, sparkSession)
    (input1, input2)
  }

  /**
   * ジョブ設計書の処理内容を実装する。
   *
   * @param input1       inputFile1で定義したDatasetが渡される。
   * @param input2       inputFile1で定義したDatasetが渡される。
   * @param sparkSession SparkSession。当該メソッド内でDatasetを扱うために
   * {{{ import sparkSession.implicits._ }}}
   *                     を指定できる。
   * @return ジョブの処理結果となるDataset
   */
  def process(input1: Dataset[T1], input2: Dataset[T2], sparkSession: SparkSession): Dataset[U]

  /**
   * processメソッドで返却されたDatasetからoutputFileのDataFileで
   * 指定されたファイルを出力する
   *
   * @param ds processメソッドで返却されたDataset
   */
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
    logInfo(Message.get(FWMsgConst.I_FW_002, getClass().getTypeName()))
  }

}
