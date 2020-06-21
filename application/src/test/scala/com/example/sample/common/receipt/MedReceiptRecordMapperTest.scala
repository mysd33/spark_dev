package com.example.sample.common.receipt

import org.scalatest.funsuite.AnyFunSuite

/**
 * Mapperのテストの例
 */
class MedReceiptRecordMapperTest extends AnyFunSuite{
  test("MedReceiptRecordMapper test"){
    //入力データ（この例では、文字列を渡しているが、ファイルから取得するようにしてよい）
    val input = "2,1,0,MN,910000001,東京都港区新橋,13143005910000001,,,\r\n" +
      "1,3,0,RE,2,1117,43004,サンプル　二,1,3180717,,4300409,,,,,,,,,,,,,06,,,,,,,,,,,,,,,,\r\n"

    //期待値(MN)
    val expectedMN = MedMN(dataShikibetsu = "2", gyoNo = "1", receEdaNo = "0", recordType = "MN", receiptKanriNo = "910000001",
      iryokikannAddress = "東京都港区新橋", yobi1= "13143005910000001", yobi2 = "", yobi3 = "", yobi4 = "")

    //期待値(RE)
    val expectedRE = MedRE(receiptKanriNo = "910000001", dataShikibetsu = "1", gyoNo = "3", receEdaNo = "0",
      recordType = "RE", receiptNo = "2", receiptType = "1117", shinryoMonth = "43004", name = "サンプル　二",
      sex = "1", birthday = "3180717",kyufuWariai = "", nyuinDate = "4300409", byotoKubun = "", futanKubun = "",
      tokkijiko = "", byoshosu = "", karteNo = "", waribikiTensuTanka = "", yobi1 = "", yobi2 = "", yobi3 = "",
      kensakuNo = "", kirokujokenMonth = "", seikyu = "",
      shinryokaName1 = "06", bui1 = "", shinryoukaSex1 = "", shochi1 = "", shippei1 = "",
      shinryokaName2 = "", bui2 = "", shinryoukaSex2 = "", shochi2 = "", shippei2 = "",
      shinryokaName3 = "", bui3 = "", shinryoukaSex3 = "", shochi3 = "", shippei3 = "",
      kana = "", patientStatus = "")
    val expected = Map("MN" -> expectedMN, "RE" -> expectedRE)

    //テスト対象メソッドの実行
    val actual = MedReceiptRecordMapper.mapToReceiptRecordTuples(input)

    //アサーション
    actual.foreach(i => {
      //各レセプトレコードに正しい値が含まれているかテスト
      assert(i._2 == expected(i._1))
    })


  }
}
