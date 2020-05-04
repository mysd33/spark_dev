package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.RDDToDataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataFile, MultiFormatCsvModel}
import com.example.fw.domain.utils.OptionImplicit._
import com.example.sample.common.receipt.{MedMN, MedRE, ReceiptRecord, MedReceiptRecordMapper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class SampleMedReceiptRDDBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends RDDToDataFrameBLogic(dataFileReaderWriter) {
  val outputDirPath = "receipt/output/"
  val re = "RE"
  val mn = "MN"
  var cached: RDD[(String, ReceiptRecord)] = null

  //事前にシェルで\x00で区切り文字として設定しておいたレセプトファイル
  override val inputFiles: Seq[DataFile[String]] =
    MultiFormatCsvModel[String](path = "receipt/11_RECODEINFO_MED_result.CSV",
      encoding = "MS932") :: Nil

  override val outputFiles: Seq[DataFile[Row]] =
    CsvModel[Row](outputDirPath + re
    ) :: CsvModel[Row](outputDirPath + mn
    ) :: Nil

  override def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): Seq[DataFrame] = {
    import sparkSession.implicits._
    val receipts = inputs(0)
    val result = receipts.flatMap(receipt => {
      MedReceiptRecordMapper.mapToReceiptRecordTuples(receipt)
    })
    //レコード種別ごとにDatasetを作成しファイル出力
    //何度も使用するのでキャッシュしておく
    cached = result.cache()
    val reDF = MedReceiptRecordMapper.extractRecordRDD[MedRE](re, cached).toDF()
    val mnDF = MedReceiptRecordMapper.extractRecordRDD[MedMN](mn, cached).toDF()
    //TODO:レコード種別ごとに処理を追加していく
    reDF :: mnDF :: Nil
  }

  override def tearDown(sparkSession: SparkSession): Unit = {
    //キャッシュを削除
    cached.unpersist()
    super.tearDown(sparkSession)
  }

}



