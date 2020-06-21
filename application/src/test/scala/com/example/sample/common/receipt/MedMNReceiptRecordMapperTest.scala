package com.example.sample.common.receipt

import org.scalatest.funsuite.AnyFunSuite

/**
 * ReceptRecordの各種マッピング定義のテスト
 */
class MedMNReceiptRecordMapperTest extends AnyFunSuite {
  test("MedMNReceiptRecordMapper Test") {
    //入力
    val input = Array("2", "1", "0", "MN", "910000001", "東京都港区新橋", "13143005910000001", "", "", "")
    //期待値
    val expected = MedMN(dataShikibetsu = "2", gyoNo = "1", receEdaNo = "0", recordType = "MN", receiptKanriNo = "910000001",
      iryokikannAddress = "東京都港区新橋", yobi1= "13143005910000001", yobi2 = "", yobi3 = "", yobi4 = "")

    //テスト対象メソッド実行
    val actual = MedMNReceiptRecordMapper.map(input)

    //アサーション
    assert(expected == actual)
  }
}
