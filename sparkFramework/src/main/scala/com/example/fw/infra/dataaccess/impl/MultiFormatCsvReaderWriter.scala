package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.const.FWConst
import com.example.fw.domain.model.MultiFormatCsvModel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * MultiFormatCsvModelに対応したファイルアクセス機能を提供するクラス
 *
 * SparkのnewAPIHadoopFileメソッドに対応し
 * 改行コード以外、例えば、制御コードNUL(\\0000)のような存在しない文字を区切り文字とした
 * ファイル読み込みRDDとして返却可能である。
 * このため、入力ファイルにはあらかじめシェルのsedコマンド等で、区切り文字を挿入しておくこと。
 *
 * @constructor コンストラクタ
 */
class MultiFormatCsvReaderWriter {

  /**
   * マルチフォーマット形式のCSVファイルを読み込み
   * 複数行を1要素とするRDDを返却する
   *
   * @param input        入力のMultiFormatCsvModel
   * @param sparkSession SparkSession
   * @return RDD
   */
  def readToRDD(input: MultiFormatCsvModel[String], sparkSession: SparkSession): RDD[String] = {
    val sc = sparkSession.sparkContext
    val conf = new Configuration(sc.hadoopConfiguration)
    input.recordDelimiter match {
      case Some(delimiter) => conf.set("textinputformat.record.delimiter", delimiter)
      case _ => FWConst.DEFAULT_MULTIFORMAT_CSV_DELIMITER
    }

    //参考： https://stackoverflow.com/questions/25259425/spark-reading-files-using-different-delimiter-than-new-line
    //エンコーディングをShift_JIS（MS932）に変更し、複数行を１要素とするRDD[String]を取得
    val rdd = sc.newAPIHadoopFile(input.absolutePath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
    rdd.map { case (_, text) =>
      input.encoding match {
        case Some(encoding) => new String(text.getBytes, encoding)
        case _ => new String(text.getBytes)
      }
    }
  }
}
