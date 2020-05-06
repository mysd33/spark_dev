package com.example.fw.domain.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.model.DataFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
 * 入力ファイルをRDD、出力ファイルをDataFrameで扱うためのLogicの基底クラス。
 *
 * 業務開発者は本クラスを継承してLogicクラスを実装する。
 *
 * 入力ファイルを文字列として扱い加工し表形式のファイルへ変換する場合での利用を想定している。
 * このため、入力ファイルを、DataFile[String]、RDD[String]で扱う必要がある。
 * 入力RDD、出力DataFrameともに複数ファイル扱うことができ汎用的に利用できる。
 *
 * @constructor コンストラクタ
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 */
abstract class RDDToDataFrameBLogic (val dataFileReaderWriter: DataFileReaderWriter) extends Logic {
  /** 入力ファイルのDataFileのリストを実装する。複数ファイル指定できる。 */
  val inputFiles: Seq[DataFile[String]]
  /** 出力ファイルのDataFileのリストを実装する。複数ファイル指定できる。 */
  val outputFiles: Seq[DataFile[Row]]

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
    logInfo("ビジネスロジック開始:" + getClass().getTypeName())
  }

  /**
   * inputFilesのDataFileのリストからRDDのリストを取得する
   * @param sparkSession  SparkSession
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
   * @param inputs inputFilesで定義したDataListのリストの順番にRDDのリストが渡される。
   * @param sparkSession SparkSession。当該メソッド内でDatasetを扱うために
   *                     {{{ import sparkSession.implicits._ }}}
   *                     を指定できる。
   * @return ジョブの処理結果となるDataFrame。outputFilesで定義したDataListのリストの順番にDataFrameのリストを渡す必要がある。
   */
  def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): Seq[DataFrame]

  /**
   * processメソッドで返却されたDataFrameのリストからoutputFilesのDataFileのリストで指定されたファイルを出力する
   * @param outputs processメソッドで返却されたDataFrame
   */
  final def output(outputs: Seq[DataFrame]): Unit = {
    outputs.zip(outputFiles).foreach(tuple => {
      val df = tuple._1
      val outputFile = tuple._2
      dataFileReaderWriter.writeFromDf(df, outputFile)
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
    logInfo("ビジネスロジック終了")
  }
}