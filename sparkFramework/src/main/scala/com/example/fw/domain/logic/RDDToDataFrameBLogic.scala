package com.example.fw.domain.logic

import com.example.fw.domain.const.FWMsgConst
import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.message.Message
import com.example.fw.domain.model.DataModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
 * 入力ファイルをRDD、出力ファイルをDataFrameで扱うためのLogicの基底クラス。
 *
 * 業務開発者は本クラスを継承してLogicクラスを実装する。
 *
 * 入力ファイルを文字列として扱い加工し表形式のファイルへ変換する場合での利用を想定している。
 * このため、入力ファイルを、DataModel[String]、RDD[String]で扱う必要がある。
 * 入力RDD、出力DataFrameともに複数ファイル扱うことができ汎用的に利用できる。
 *
 * @constructor コンストラクタ
 * @param dataModelReaderWriter Logicクラスが使用するDataModelReaderWriter
 * @param args                 AP起動時の引数
 */
abstract class RDDToDataFrameBLogic(val dataModelReaderWriter: DataModelReaderWriter,
                                    val args: Array[String] = null) extends Logic {
  require(dataModelReaderWriter != null)
  /** 入力ファイルのDataModelのリストを実装する。複数ファイル指定できる。 */
  val inputModels: Seq[DataModel[String]]
  /** 出力ファイルのDataModelのリストを実装する。複数ファイル指定できる。 */
  val outputModels: Seq[DataModel[Row]]

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
   * inputModelsのDataModelのリストからRDDのリストを取得する
   *
   * @param sparkSession SparkSession
   * @return RDDのリスト
   */
  final def input(sparkSession: SparkSession): Seq[RDD[String]] = {
    inputModels.map(
      inputModel => {
        dataModelReaderWriter.readToRDD(inputModel, sparkSession)
      }
    )
  }

  /**
   * ジョブ設計書の処理内容を実装する。
   *
   * @param rddList       inputModelsで定義したDataListのリストの順番にRDDのリストが渡される。
   * @param sparkSession SparkSession。当該メソッド内でDatasetを扱うために
   * {{{ import sparkSession.implicits._ }}}
   *                     を指定できる。
   * @return ジョブの処理結果となるDataFrame。outputModelsで定義したDataListのリストの順番にDataFrameのリストを渡す必要がある。
   */
  def process(rddList: Seq[RDD[String]], sparkSession: SparkSession): Seq[DataFrame]

  /**
   * processメソッドで返却されたDataFrameのリストからoutputModelsのDataModelのリストで指定されたファイルを出力する
   *
   * @param dfList processメソッドで返却されたDataFrame
   */
  final def output(dfList: Seq[DataFrame]): Unit = {
    dfList.zip(outputModels).foreach(tuple => {
      val df = tuple._1
      val outputFile = tuple._2
      dataModelReaderWriter.writeFromDf(df, outputFile)
    })
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