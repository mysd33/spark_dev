package com.example.sample.logic

import java.nio.charset.StandardCharsets

import com.example.fw.domain.logic.Logic
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

class SampleReceiptRDDBLogic extends Logic {
  //事前にシェルで\x00で区切り文字として設定しておいたレセプトファイル
  //TODO: DataFile定義化
  val inputFile = "C:\\temp\\receipt\\11_RECODEINFO_MED_result.CSV"
  //レセプトはShift_JIS（MS932）
  private val charset = "MS932"

  override def execute(sparkSession: SparkSession): Unit = {
    val charset_temp = charset
    //TODO: AP基盤でラップしたAPIを用意
    // 改行コード以外、例えば、制御コードNUL(\x00)のような存在しない文字を区切り文字としてレセプトをファイル読み込み
    // https://stackoverflow.com/questions/25259425/spark-reading-files-using-different-delimiter-than-new-line
    val sc = sparkSession.sparkContext
    val conf = new Configuration(sc.hadoopConfiguration)
    // \x00で区切り文字として設定
    conf.set("textinputformat.record.delimiter", "\u0000")
    val rdd = sc.newAPIHadoopFile(inputFile, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
    //エンコーディングをShift_JIS（MS932）に変更し、各レセプトの文字列を１要素とするRDD[String]を取得
    val receipts = rdd.map { case (_, text) => new String(text.getBytes, charset_temp) }

    //レセプトの１つ目を取得
    receipts.take(1).foreach(println)
    //TODO:ビジネスロジックで、レコードごとに分解してファイルを作成

  }
}
