package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.RDDToDataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataFile, MultiFormatCsvModel}
import com.example.fw.domain.utils.OptionImplicit._
import com.example.sample.common.receipt.{MedMNReceiptRecord, MedREReceiptRecord, ReceiptRecord, ReceiptRecordMapper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class SampleMedReceiptRDDBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends RDDToDataFrameBLogic(dataFileReaderWriter) {
  val outputDirPath = "receipt/output"
  val directoryPath = "/"
  val re = "RE"
  val mn = "MN"
  var cached: RDD[(String, ReceiptRecord)] = null

  //事前にシェルで\x00で区切り文字として設定しておいたレセプトファイル
  override val inputFiles: Seq[DataFile[String]] =
    MultiFormatCsvModel[String](path = "receipt/11_RECODEINFO_MED_result.CSV",
      encoding = "MS932") :: Nil

  override val outputFiles: Seq[DataFile[Row]] =
    CsvModel[Row](outputDirPath + directoryPath + re
    ) :: CsvModel[Row](outputDirPath + directoryPath + mn
    ) :: Nil


  //override def process(receipts: RDD[String], sparkSession: SparkSession): RDD[(String, ReceiptRecord)] = {
  override def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): Seq[DataFrame] = {
    import sparkSession.implicits._
    val lineSeparator = "\r\n"
    val receipts = inputs(0)
    val result = receipts.flatMap(receipt => {
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
    //レコード種別ごとにDatasetを作成しファイル出力
    //明示的に各クラスをキャストして出力しないといけないコードが冗長だが、対処が難しい
    //何度も使用するのでキャッシュしておく
    cached = result.cache()
    //TODO: レセプト解析コンポーネントとして別クラスに切り出してテストしやすくする
    val reDS = cached.filter(t => t._1 == re)
      .map(t => t._2.asInstanceOf[MedREReceiptRecord])
      .toDF()
    val mnDS = cached.filter(t => t._1 == mn)
      .map(t => t._2.asInstanceOf[MedMNReceiptRecord])
      .toDF()
    reDS :: mnDS :: Nil
  }

  override def tearDown(sparkSession: SparkSession): Unit = {
    //キャッシュをunpersist
    cached.unpersist()
    super.tearDown(sparkSession)
  }

}



