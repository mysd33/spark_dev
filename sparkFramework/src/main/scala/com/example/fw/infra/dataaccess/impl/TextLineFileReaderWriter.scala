package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.TextLineModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

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
    val rdd = sparkSession.sparkContext.textFile(inputFile.absolutePath)
    //encoding
    inputFile.encoding match {
      case Some(encoding) => rdd.map(s => new String(s.getBytes, encoding))
      case _ => rdd
    }
  }

  def readToDf(inputFile: TextLineModel[Row], sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    val df = sparkSession.read.text(inputFile.absolutePath)
    //encoding
    inputFile.encoding match {
      case Some(encoding) => df.map(row => {
        val s = row.getAs[String]("value")
        new String(s.getBytes(), encoding)
      }).toDF()
      case _ => df
    }

  }

  def readToDs(inputFile: TextLineModel[String], sparkSession: SparkSession): Dataset[String] = {
    import sparkSession.implicits._
    val ds = sparkSession.read.textFile(inputFile.absolutePath)
    //encoding
    inputFile.encoding match {
      case Some(encoding) => ds.map(s => new String(s.getBytes, encoding))
      case _ => ds
    }
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
