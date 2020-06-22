package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.RDDToDataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataModel, MultiFormatCsvModel}
import com.example.fw.domain.utils.OptionImplicit._
import com.example.sample.common.receipt.{MedMN, MedRE, MedReceiptRecordMapper, ReceiptConst, ReceiptRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * AP基盤を使ったサンプル
 *
 * RDDToDataFrameBLogicを継承し、
 * 事前にシェルで\x00で行の区切り文字として設定しておいたレセプトファイル（レセ電コード情報）の読み込み
 * を試行してみた例
 *
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 */
class SampleMedReceiptRDDBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends RDDToDataFrameBLogic(dataFileReaderWriter) {

  //事前にシェルで\x00で区切り文字として設定しておいたレセプトファイル（レセ電コード情報）の読み込みの例
  override val inputFiles: Seq[DataModel[String]] =
    MultiFormatCsvModel[String](relativePath = "receipt/11_RECODEINFO_MED_result.CSV",
      //Shift_JIS形式
      encoding = ReceiptConst.ReceiptEncoding) :: Nil

  //レセプトをレコード種別ごとにCSVファイルで出力する例。出力時にbzip2で圧縮。
  private val outputDirPath = "receipt/output/"
  override val outputFiles: Seq[DataModel[Row]] =
    CsvModel[Row](outputDirPath + ReceiptConst.MN, compression = "bzip2"
    ) :: CsvModel[Row](outputDirPath + ReceiptConst.RE, compression = "bzip2"
    ) :: Nil
  //TODO: 全レコード種別を追加していく

  //キャッシュを保持
  private var cached: RDD[(String, ReceiptRecord)] = null

  override def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): Seq[DataFrame] = {
    import sparkSession.implicits._
    //入力ファイルのRDDを取得。複数レセプトを要素として含んでいる。
    val receipts = inputs(0)

    val result = receipts.flatMap(receipt => {
      //1レセプトを、(レコード種別文字列,ReceiptRecord)のタプルにマッピング
      MedReceiptRecordMapper.mapToReceiptRecordTuples(receipt)
    })
    //レコード種別ごとにDatasetを作成しファイル出力
    //何度も使用するのでキャッシュしておく
    cached = result.cache()
    //TODO:DatasetをDataFrameに変換しないといけないのがイケてない
    val mnDF = MedReceiptRecordMapper.extractRDD[MedMN](ReceiptConst.MN, cached).toDF()
    val reDF = MedReceiptRecordMapper.extractRDD[MedRE](ReceiptConst.RE, cached).toDF()

    //TODO:レコード種別ごとに処理を追加していく
    mnDF :: reDF :: Nil
  }

  override def tearDown(sparkSession: SparkSession): Unit = {
    //キャッシュを削除
    cached.unpersist()
    super.tearDown(sparkSession)
  }

}



