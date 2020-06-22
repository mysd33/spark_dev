package com.example.fw.domain.logic

import com.example.fw.domain.const.FWMsgConst
import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.message.Message
import com.example.fw.domain.model.DataModel
import javassist.bytecode.stackmap.TypeTag
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 入出力ファイルをRDDで扱うためのLogicの基底クラス。
 *
 * 業務開発者は本クラスを継承してLogicクラスを実装する。
 *
 * 入力ファイルを文字列として扱い加工し表形式のファイルへ変換する場合での利用を想定している。
 * このため、入力ファイルを、DataFile[String]、RDD[String]で扱う必要がある。
 * 入力RDDは複数ファイル扱うことができ汎用的に利用できるが、
 * 出力RDDの型パラメータを指定する必要があるため、
 * 複数のRDDに対して1つのRDDを返却する場合のみ利用できる。
 *
 * @constructor コンストラクタ
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 * @param args                 AP起動時の引数
 * @tparam U 出力ファイルつまり、outputFileのDataFile、RDDが扱う型パラメータ
 */
abstract class RDDToRDDBLogic[U](val dataFileReaderWriter: DataFileReaderWriter,
                                 val args: Array[String] = null) extends Logic {
  require(dataFileReaderWriter != null)
  /** 入力ファイルのDataFileのリストを実装する。複数ファイル指定できる。 */
  val inputFiles: Seq[DataModel[String]]
  /** 出力ファイルのDataFileを実装する。 1ファイルのみ指定できる。 */
  val outputFile: DataModel[U]

  /**
   * @see [[com.example.fw.domain.logic.Logic.execute]]
   * @param sparkSession SparkSession
   */
  override final def execute(sparkSession: SparkSession): Unit = {
    try {
      setUp(sparkSession)
      val inputDatasets = input(sparkSession)
      val outputDatasets = process(inputDatasets, sparkSession)
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
   * inputFilesのDataFileのリストからRDDのリストを取得する
   *
   * @param sparkSession SparkSession
   * @return RDDのリスト
   */
  final def input(sparkSession: SparkSession): Seq[RDD[String]] = {
    inputFiles.map(
      inputFile => {
        dataFileReaderWriter.readToRDD(inputFile, sparkSession)
      }
    )
  }

  /**
   * ジョブ設計書の処理内容を実装する。
   *
   * @param inputs       inputFilesで定義したDataListのリストの順番にRDDのリストが渡される。
   * @param sparkSession SparkSession
   * @return ジョブの処理結果となるRDD
   */
  def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): RDD[U]

  /**
   * processメソッドで返却されたRDDからoutputFileのDataFileで
   * 指定されたファイルを出力する
   *
   * @param rdd processメソッドで返却されたRDD
   */
  final def output(rdd: RDD[U]): Unit = {
    dataFileReaderWriter.writeFromRDD(rdd, outputFile)
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
