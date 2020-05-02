package com.example.sample.common.receipt

object ReceiptRecordParser {
  private val delimiter = ","

  def parse(recordString: String): ReceiptRecord = {
    //TODO:値がない場合のためlimit=-1
    val items = recordString.split(delimiter, -1)
    val unKnownRecord = UnknownReceiptRecord(items)
    val receiptRecord =
      unKnownRecord.recordType match {
        case "MN" => MedMNReceiptRecordMapper.map(items)
        case "RE" => MedREReceiptRecordMapper.map(items)

        //TODO:レコード種別ごとに、文字列配列をcaseクラスにマッピングする処理を追加

        case _ => unKnownRecord
      }
    receiptRecord

  }
}
