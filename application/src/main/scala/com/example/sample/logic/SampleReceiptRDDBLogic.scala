package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.Logic
import com.example.fw.domain.model.{CsvModel, DataFile, MultiFormatCsvModel}
import com.example.sample.common.logic.AbstractReceiptRDDBLogic
import com.example.sample.common.receipt.{MedMNReceiptRecord, MedREReceiptRecord, ReceiptRecord, ReceiptRecordMapper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class SampleReceiptRDDBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends AbstractReceiptRDDBLogic(dataFileReaderWriter) {
  //事前にシェルで\x00で区切り文字として設定しておいたレセプトファイル
  override val inputFile: DataFile[String] =
    MultiFormatCsvModel[String](path = "receipt/11_RECODEINFO_MED_result.CSV", ec = "MS932")

  override def process(receipts: RDD[String], sparkSession: SparkSession): RDD[(String, ReceiptRecord)] = {
    val lineSeparator = "\r\n"
    receipts.flatMap(receipt => {
      //1レセプトを改行コードでレコードごとに分解
      val records = receipt.split(lineSeparator)
      //最初のMNレコードからレセプト管理番号を取得
      val receiptKanriNo = ReceiptRecordMapper.map(null, records(0))
        .asInstanceOf[MedMNReceiptRecord]
        .receiptKanriNo
      //レコードごとのRDDを作成
      records.map(recordString => {
        //レコードの種類ごとにファイル分解する前に
        //どのレセプトかを分かるキーとしてレセプト管理番号を入れておく
        val record = ReceiptRecordMapper.map(receiptKanriNo, recordString)
        (record.recordType, record)
      })
    })
  }

  override def output(result: RDD[(String, ReceiptRecord)], sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val outputDirPath = "receipt/output"
    val directoryPath = "/"

    //何度も使用するのでキャッシュしておく
    val cached = result.cache()
    try {
      //レコード種別ごとにDatasetを作成しファイル出力
      //TODO: 明示的に各クラスをキャストして出力しないといけないコードが冗長だが、対処が難しい
      val re = "RE"
      val reDir = CsvModel[MedREReceiptRecord](outputDirPath + directoryPath + re)
      val reDS = cached.filter(t => t._1 == re)
        .map(t => t._2.asInstanceOf[MedREReceiptRecord])
        .toDS()
      dataFileReaderWriter.writeFromDs(reDS, reDir)

      val mn = "MN"
      val mnDir = CsvModel[MedMNReceiptRecord](outputDirPath + directoryPath + mn)
      val mnDS = cached.filter(t => t._1 == mn)
        .map(t => t._2.asInstanceOf[MedMNReceiptRecord])
        .toDS()
      dataFileReaderWriter.writeFromDs(mnDS, mnDir)
    } finally {
      cached.unpersist()
    }
  }
}



