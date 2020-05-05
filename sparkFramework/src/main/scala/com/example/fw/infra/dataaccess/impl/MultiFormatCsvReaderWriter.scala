package com.example.fw.infra.dataaccess.impl


import com.example.fw.domain.const.FWConst
import com.example.fw.domain.model.MultiFormatCsvModel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class MultiFormatCsvReaderWriter  {
  def readToRDD(inputFile: MultiFormatCsvModel[String], sparkSession: SparkSession): RDD[String] = {
    // 改行コード以外、例えば、制御コードNUL(\x00)のような存在しない文字を区切り文字としてレセプトをファイル読み込み
    // https://stackoverflow.com/questions/25259425/spark-reading-files-using-different-delimiter-than-new-line
    val sc = sparkSession.sparkContext
    val conf = new Configuration(sc.hadoopConfiguration)
    inputFile.delimiter match {
      case Some(delimiter) => conf.set("textinputformat.record.delimiter", delimiter)
      case _ => FWConst.DEFAULT_MULTIFORMAT_CSV_DELIMITER
    }

    //エンコーディングをShift_JIS（MS932）に変更し、各レセプトの文字列を１要素とするRDD[String]を取得
    val rdd = sc.newAPIHadoopFile(inputFile.absolutePath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
    rdd.map { case (_, text) =>
      inputFile.encoding match {
        case Some(encoding) => new String(text.getBytes, encoding)
        case _ => new String(text.getBytes)
      }
    }
  }
}
