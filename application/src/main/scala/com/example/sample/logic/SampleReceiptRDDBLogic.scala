package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.Logic
import com.example.fw.domain.model.{CsvModel, DataFile, MultiFormatCsvModel}
import com.example.sample.common.receipt.{MedMNReceiptRecord, MedREReceiptRecord, ReceiptRecord, ReceiptRecordParser}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class SampleReceiptRDDBLogic(val dataFileReaderWriter: DataFileReaderWriter) extends Logic {
  //事前にシェルで\x00で区切り文字として設定しておいたレセプトファイル
  val inputFile: DataFile[String] =
    MultiFormatCsvModel[String](path = "receipt/11_RECODEINFO_MED_result.CSV", ec = "MS932")

  val outputDirPath = "receipt/output"

  override def execute(sparkSession: SparkSession): Unit = {
    val receipts = input(sparkSession)
    val result = process(receipts, sparkSession)
    output(result, sparkSession)
  }

  def input(sparkSession: SparkSession): RDD[String] = {
    //レセプト単位の要素とするRDDを返却
    dataFileReaderWriter.readToRDD(inputFile, sparkSession)
  }

  def process(receipts: RDD[String], sparkSession: SparkSession): RDD[(String, ReceiptRecord)] = {

    val lineSeparator = "\r\n"
    receipts.flatMap(receipt => {
      //1レセプトをレコードごとに分解
      val records = receipt.split(lineSeparator)
      //最初のMNレコードからレセプト管理番号を取得
      val receiptKanriNo = ReceiptRecordParser.parse(records(0))
        .asInstanceOf[MedMNReceiptRecord]
        .receiptKanriNo

      records.map(recordString => {
        //TODO:ファイル分解する前にレセプト管理番号を入れておく
        val record = ReceiptRecordParser.parse(recordString)
        (record.recordType, record)
      })
    }).cache() //何度も使用するのでキャッシュしておく
  }

  def output(result: RDD[(String, ReceiptRecord)], sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    //レコード種別ごとにDatasetを作成しファイル出力
    //TODO: 明示的に各クラスをキャストして出力しないといけないコードが冗長だが、対処が難しい
    val directoryPath = "/"

    val re = "RE"
    val reDir = CsvModel[MedREReceiptRecord](outputDirPath + directoryPath + re)
    val reDS = result.filter(t => t._1 == re)
      .map(t => t._2.asInstanceOf[MedREReceiptRecord])
      .toDS()
    dataFileReaderWriter.writeFromDs(reDS, reDir)

    val mn = "MN"
    val mnDir = CsvModel[MedMNReceiptRecord](outputDirPath + directoryPath + mn)
    val mnDS = result.filter(t => t._1 == mn)
      .map(t => t._2.asInstanceOf[MedMNReceiptRecord])
      .toDS()
    dataFileReaderWriter.writeFromDs(mnDS, mnDir)


  }

}



