package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataFile, MultiFormatCsvModel}
import com.example.sample.common.receipt.{MedMN, MedRE, MedReceiptRecordMapper, ReceiptConst, ReceiptRecord}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import com.example.fw.domain.utils.OptionImplicit._


import scala.collection.immutable.Nil

/**
 * AP基盤を使ったサンプル
 *
 * DataFrameBLogicを継承し、
 * 事前にシェルで、レセプト区切り文字は改行コードのまま、
 * 各レセプトレコードの区切り文字をとして\x00で設定しておいたレセプトファイル（レセ電コード情報）の読み込み
 * を試行してみた例
 * RDDを使わずにDataFrame/Datasetのみで実現している
 *
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 */
class SampleMedReceiptDataFrameBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends DataFrameBLogic(dataFileReaderWriter) {

  //TODO: MultiFormatCsvModelのDataFrame対応の実装
  override val inputFiles: Seq[DataFile[Row]] = MultiFormatCsvModel[Row](
    //TODO: テストデータの作成
    relativePath = "receipt/11_RECODEINFO_MED_result2.CSV",
    //Shift_JIS形式
    encoding = ReceiptConst.ReceiptEncoding) :: Nil

  //レセプトをレコード種別ごとにCSVファイルで出力する例。出力時にbzip2で圧縮。
  private val outputDirPath = "receipt/output2/"
  override val outputFiles: Seq[DataFile[Row]] =
    CsvModel[Row](outputDirPath + ReceiptConst.MN, compression = "bzip2"
    ) :: CsvModel[Row](outputDirPath + ReceiptConst.RE, compression = "bzip2"
    ) :: Nil
  //TODO: 全レコード種別を追加していく

  //キャッシュを保持
  private var cached: Dataset[(String, ReceiptRecord)] = null

  override def process(inputs: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame] = {
    import sparkSession.implicits._
    val receipts = inputs(0)
    val result = receipts.flatMap(receipt => {
      //1レセプトを、(レコード種別文字列,ReceiptRecord)のタプルにマッピング
      MedReceiptRecordMapper.mapToReceiptRecordTuples(receipt.getAs[String]("value"))
    })
    //レコード種別ごとにDataset/DataFrameを作成しファイル出力
    //何度も使用するのでキャッシュしておく
    cached = result.cache()

    //TODO:DatasetをDataFrameに変換しないといけないのがイケてない
    val mnDF = MedReceiptRecordMapper.extractDataset[MedMN](ReceiptConst.MN, cached, sparkSession).toDF()
    val reDF = MedReceiptRecordMapper.extractDataset[MedRE](ReceiptConst.RE, cached, sparkSession).toDF()

    //TODO:レコード種別ごとに処理を追加していく
    mnDF :: reDF :: Nil
  }
}
