package com.example.fw.domain.logic

import com.example.fw.domain.const.FWMsgConst
import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.message.Message
import com.example.fw.domain.model.DataModel
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * 入出力をDatasetで扱うためのLogicの基底クラス。
 *
 * 業務開発者は本クラスを継承してLogicクラスを実装する。
 *
 * Datasetを使用できるのでタイプセーフに実装できるが
 * 型パラメータを指定する必要があるため、
 * 1つの入力Datasetに対して1つのDatasetを返却する場合のみ利用できる。
 *
 * @constructor コンストラクタ
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 * @param args                 AP起動時の引数
 * @tparam T 入力ファイルつまり、inputFileのDataFile、Datasetが扱う型パラメータ
 * @tparam U 出力ファイルつまり、outputFileのDataFile、Datasetが扱う型パラメータ
 */
abstract class DatasetBLogic1to1[T <: Product : TypeTag, U <: Product : TypeTag]
(val dataFileReaderWriter: DataFileReaderWriter, val args: Array[String] = null) extends Logic {
  require(dataFileReaderWriter != null)
  /** 入力ファイルのDataFileを実装する。 1ファイルのみ指定できる。 */
  val inputFile: DataModel[T]
  /** 出力ファイルのDataFileを実装する。 1ファイルのみ指定できる。 */
  val outputFile: DataModel[U]

  /**
   * @see [[com.example.fw.domain.logic.Logic.execute]]
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
    logInfo(Message.get(FWMsgConst.I_FW_001, getClass().getTypeName()))
  }

  /**
   * inputFilesのDataFileからDatasetを取得する
   *
   * @param sparkSession SparkSession
   * @return Dataset
   */
  final def input(sparkSession: SparkSession): Dataset[T] = {
    dataFileReaderWriter.readToDs(inputFile, sparkSession)
  }

  /**
   * ジョブ設計書の処理内容を実装する。
   *
   * @param input        inputFileで定義したDatasetが渡される。
   * @param sparkSession SparkSession。当該メソッド内でDatasetを扱うために
   * {{{ import sparkSession.implicits._ }}}
   *                     を指定できる。
   * @return ジョブの処理結果となるDataset
   */
  def process(input: Dataset[T], sparkSession: SparkSession): Dataset[U]


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
