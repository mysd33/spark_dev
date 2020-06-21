package com.example.sample.common.receipt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
 * 医科レセプトのMapperクラスの例
 */
object MedReceiptRecordMapper {
  private val delimiter = ","
  private val lineSeparator = "\r\n"

  /**
   * 1レセプトの文字列を、ReceiptRecordに変換
   * @param receipt 1レセプトの文字列
   * @return
   */
  def mapToReceiptRecordTuples(receipt: String): Array[(String, ReceiptRecord)] = {
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

  private def mapToRecord(recordString: String): ReceiptRecord = mapToRecord(recordString, null)

  private def mapToRecord(recordString: String, receiptKanriNo: String): ReceiptRecord = {
    //値がない場合のためlimit=-1
    val items = recordString.split(delimiter, -1)
    val unKnownRecord = UnknownReceiptRecord(items)
    val receiptRecord =
      unKnownRecord.recordType match {
        case ReceiptConst.MN => MedMNReceiptRecordMapper.map(items)
        case ReceiptConst.RE => MedREReceiptRecordMapper.map(receiptKanriNo, items)

        //TODO:レコード種別ごとに、文字列配列をcaseクラスにマッピングする処理を追加していく

        case _ => unKnownRecord
      }
    receiptRecord
  }

  def extractRDD[T <: ReceiptRecord : ClassTag]
  (recordTypeName: String, rdd: RDD[(String, ReceiptRecord)]): RDD[T] = {
    rdd.filter(t => t._1 == recordTypeName)
      .map(t => t._2.asInstanceOf[T])
  }

  def extractDataset[T <: Product with ReceiptRecord : TypeTag]
  (recordTypeName: String, ds: Dataset[(String, ReceiptRecord)], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    ds.filter(t => t._1 == recordTypeName)
      .map(t => t._2.asInstanceOf[T])
  }

}
