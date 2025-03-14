package com.example.sample.common.receipt

/**
 * レセプトレコードクラスの例
 *
 * caseクラスで作成
 */
case class MedRE(receiptKanriNo: String,
                 dataShikibetsu: String,
                 gyoNo: String,
                 receEdaNo: String,
                 recordType: String,
                 receiptNo: String,
                 receiptType: String,
                 shinryoMonth: String,
                 name: String,
                 sex: String,
                 birthday: String,
                 kyufuWariai: String,
                 nyuinDate: String,
                 byotoKubun: String,
                 futanKubun: String,
                 tokkijiko: String,
                 byoshosu: String,
                 karteNo: String,
                 waribikiTensuTanka: String,
                 yobi1: String,
                 yobi2: String,
                 yobi3: String,
                 kensakuNo: String,
                 kirokujokenMonth: String,
                 seikyu: String,
                 shinryokaName1: String,
                 bui1: String,
                 shinryoukaSex1: String,
                 shochi1: String,
                 shippei1: String,
                 shinryokaName2: String,
                 bui2: String,
                 shinryoukaSex2: String,
                 shochi2: String,
                 shippei2: String,
                 shinryokaName3: String,
                 bui3: String,
                 shinryoukaSex3: String,
                 shochi3: String,
                 shippei3: String,
                 kana: String,
                 patientStatus: String) extends ReceiptRecord {
}

/**
 * CSVのマッピング定義
 */
object MedREReceiptRecordMapper {
  def map(receiptKanriNo: String, items: Array[String]): MedRE = {
    MedRE(
      receiptKanriNo = receiptKanriNo,
      dataShikibetsu = items(0),
      gyoNo = items(1),
      receEdaNo = items(2),
      recordType = items(3),
      receiptNo = items(4),
      receiptType = items(5),
      shinryoMonth = items(6),
      name = items(7),
      sex = items(8),
      birthday = items(9),
      kyufuWariai = items(10),
      nyuinDate = items(11),
      byotoKubun = items(12),
      futanKubun = items(13),
      tokkijiko = items(14),
      byoshosu = items(15),
      karteNo = items(16),
      waribikiTensuTanka = items(17),
      yobi1 = items(18),
      yobi2 = items(19),
      yobi3 = items(20),
      kensakuNo = items(21),
      kirokujokenMonth = items(22),
      seikyu = items(23),
      shinryokaName1 = items(24),
      bui1 = items(25),
      shinryoukaSex1 = items(26),
      shochi1 = items(27),
      shippei1 = items(28),
      shinryokaName2 = items(29),
      bui2 = items(30),
      shinryoukaSex2 = items(31),
      shochi2 = items(32),
      shippei2 = items(33),
      shinryokaName3 = items(34),
      bui3 = items(35),
      shinryoukaSex3 = items(36),
      shochi3 = items(37),
      shippei3 = items(38),
      kana = items(39),
      patientStatus = items(40))
  }
}



