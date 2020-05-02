package com.example.sample.common.receipt

object ReceiptRecordMapper {
  private val delimiter = ","

  def map(receiptKanriNo:String, recordString: String): ReceiptRecord = {
    //TODO:値がない場合のためとりあえず、limit=-1
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

}
