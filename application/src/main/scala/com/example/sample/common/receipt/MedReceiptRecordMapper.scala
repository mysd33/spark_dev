package com.example.sample.common.receipt

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object MedReceiptRecordMapper {
  private val delimiter = ","
  private val lineSeparator = "\r\n"

  def mapToReceiptRecordTuples(receipt: String):Array[(String, ReceiptRecord)] = {
    //1レセプトを改行コードでレコードごとに分解
    val records = receipt.split(lineSeparator)
    //最初のMNレコードからレセプト管理番号を取得
    val receiptKanriNo = MedReceiptRecordMapper.mapToRecord(records(0))
      .asInstanceOf[MedMN]
      .receiptKanriNo
    //レコードごとのRDDを作成
    records.map(recordString => {
      //レコードの種類ごとにファイル分解する前に
      //どのレセプトかを分かるキーとしてレセプト管理番号を入れておく
      val record = MedReceiptRecordMapper.mapToRecord(recordString, receiptKanriNo)
      (record.recordType, record)
    })
  }

  def mapToRecord(recordString: String): ReceiptRecord = mapToRecord(recordString, null)

  def mapToRecord(recordString: String, receiptKanriNo: String): ReceiptRecord = {
    //値がない場合のためlimit=-1
    val items = recordString.split(delimiter, -1)
    val unKnownRecord = UnknownReceiptRecord(items)
    val receiptRecord =
      unKnownRecord.recordType match {
        case "MN" => MedMNReceiptRecordMapper.map(items)
        case "RE" => MedREReceiptRecordMapper.map(receiptKanriNo, items)

        //TODO:レコード種別ごとに、文字列配列をcaseクラスにマッピングする処理を追加していく

        case _ => unKnownRecord
      }
    receiptRecord
  }

  def extractRecordRDD[T <: ReceiptRecord : ClassTag]
  (recordTypeName: String, rdd: RDD[(String, ReceiptRecord)]): RDD[T] = {
    rdd.filter(t => t._1 == recordTypeName)
      .map(t => t._2.asInstanceOf[T])
  }

}
