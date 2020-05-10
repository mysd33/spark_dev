package com.example.sample.common.receipt

/**
 * レセプトレコードクラスの例
 *
 * caseクラスで作成
 *
 * @param dataShikibetsu
 * @param gyoNo
 * @param receEdaNo
 * @param recordType
 * @param receiptKanriNo
 * @param iryokikannAddress
 * @param yobi1
 * @param yobi2
 * @param yobi3
 * @param yobi4
 */
case class MedMN(dataShikibetsu: String,
                 gyoNo: String,
                 receEdaNo: String,
                 recordType: String,
                 receiptKanriNo: String,
                 iryokikannAddress: String,
                 yobi1: String,
                 yobi2: String,
                 yobi3: String,
                 yobi4: String) extends ReceiptRecord {
}

/**
 * CSVのマッピング定義
 */
object MedMNReceiptRecordMapper {
  def map(items: Array[String]): MedMN = {
    MedMN(
      dataShikibetsu = items(0),
      gyoNo = items(1),
      receEdaNo = items(2),
      recordType = items(3),
      receiptKanriNo = items(4),
      iryokikannAddress = items(5),
      yobi1 = items(6),
      yobi2 = items(7),
      yobi3 = items(8),
      yobi4 = items(9))
  }
}
