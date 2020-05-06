package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.TextLineModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * TextLineModelに対応したファイルアクセス機能を提供するクラス
 *
 * SparkのtextFileメソッド、saveAsTextFileメソッドに対応
 *
 * @constructor コンストラクタ
 */
class TextLineFileReaderWriter {
  /**
   * ファイルを読み込みRDDを返却する
   *
   * @param inputFile    入力ファイルのTextLineModel
   * @param sparkSession SparkSession
   * @return RDD
   */
  def readToRDD(inputFile: TextLineModel[String], sparkSession: SparkSession): RDD[String] = {
    sparkSession.sparkContext.textFile(inputFile.absolutePath)
  }

  /**
   * 引数で受け取ったRDDを指定のファイルに出力する
   *
   * @param rdd        出力対象のRDD
   * @param outputFile 出力先ファイルのTextLineModel
   * @tparam T RDDおよびDataFileの型パラメータ
   */
  def writeFromRDD[T](rdd: RDD[T], outputFile: TextLineModel[T]): Unit = {
    rdd.saveAsTextFile(outputFile.absolutePath)
  }
}
