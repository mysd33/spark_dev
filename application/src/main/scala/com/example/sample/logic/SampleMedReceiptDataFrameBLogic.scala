package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.logic.DataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataModel}
import com.example.fw.domain.utils.OptionImplicit._
import com.example.sample.common.receipt.MedReceiptRecordMapper.{delimiter, lineSeparator}
import com.example.sample.common.receipt._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.immutable.Nil

//TODO: コンパイルは通るが、動作しないので、使えない
/**
 * AP基盤を使ったサンプル
 *
 * DataFrameBLogicを継承し、
 * 事前にシェルで、レセプト区切り文字は改行コードのまま、
 * 各レセプトレコードの区切り文字をとして\x00で設定しておいたレセプトファイル（レセ電コード情報）の読み込み
 * を試行してみた例
 *
 * @deprecated RDDを使わずにDataFrame/Datasetのみで実現しようとしたが、前処理とかCsvModelの定義とか少し煩雑なのと
 *             汎用的に処理しようとすると型パラメータの問題で、Encoderが働かず動作しないので、使えない。
 * @param dataModelReaderWriter Logicクラスが使用するDataModelReaderWriter
 */
class SampleMedReceiptDataFrameBLogic(dataModelReaderWriter: DataModelReaderWriter)
  extends DataFrameBLogic(dataModelReaderWriter) {

  //区切り文字を\u0000とするCSVファイルとみて、CsvModelとして扱う
  override val inputModels: Seq[DataModel[Row]] = CsvModel[Row](
    relativePath = "receipt/11_RECODEINFO_MED_result2.CSV",
    sep = "\u0000",
    schema = StructType(
      Array(
        StructField("value", StringType, false)
      )
    ),
    //Shift_JIS形式
    encoding = ReceiptConst.ReceiptEncoding) :: Nil

  //レセプトをレコード種別ごとにCSVファイルで出力する例。出力時にbzip2で圧縮。
  private val outputDirPath = "receipt/output2/"
  override val outputModels: Seq[DataModel[Row]] =
    CsvModel[Row](outputDirPath + ReceiptConst.MN, compression = "bzip2"
    ) :: CsvModel[Row](outputDirPath + ReceiptConst.RE, compression = "bzip2"
    ) :: Nil
  //TODO: 全レコード種別を追加していく

  //キャッシュを保持
  private var cached: Dataset[(String, ReceiptRecord)] = null

  override def process(dfList: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame] = {
    import sparkSession.implicits._
    val receipts = dfList(0)
    val result = receipts.flatMap(row => {

      //TODO: 具体的なcaseクラスでジェネリクスを扱えないのに、Datasetで扱おうとして、(string, ReceiptReord)のEncoderがないというエラーが出てしまい動作しない
      // java.lang.UnsupportedOperationException: No Encoder found for com.example.sample.common.receipt.ReceiptRecord
      //　- field (class: "com.example.sample.common.receipt.ReceiptRecord", name: "_2")
      //  - root class: "scala.Tuple2"
      //1レセプトを、(レコード種別文字列,ReceiptRecord)のタプルにマッピング
      MedReceiptRecordMapper.mapToReceiptRecordTuples(row.getAs[String]("value"))
    })

    //レコード種別ごとにDataset/DataFrameを作成しファイル出力
    //何度も使用するのでキャッシュしておく
    cached = result.cache()

    //DatasetをDataFrameに変換しないといけないのがイケてない
    val mnDF = MedReceiptRecordMapper.extractDataset[MedMN](ReceiptConst.MN, cached, sparkSession).toDF()
    val reDF = MedReceiptRecordMapper.extractDataset[MedRE](ReceiptConst.RE, cached, sparkSession).toDF()

    //TODO: レコード種別ごとに処理を追加していく
    mnDF :: reDF :: Nil
  }
}
