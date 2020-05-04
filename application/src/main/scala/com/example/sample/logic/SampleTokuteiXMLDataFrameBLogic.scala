package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.RDDToDataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataFile, TextFileModel}
import com.example.sample.common.tokutei.{Code, CodeTokuteiKenshinMapper, PatientRole, PatientRoleTokuteiKenshinMapper, TokuteiKenshin, TokuteiKenshinConst, TokuteiKenshinMapper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.xml.XML

class SampleTokuteiXMLDataFrameBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends RDDToDataFrameBLogic(dataFileReaderWriter) {
  private val outputDir = "tokutei/output2/"

  //1行1特定検診XMLのテキストファイルとして扱う
  override val inputFiles: Seq[DataFile[String]] =
    TextFileModel[String]("tokutei/kensin_kihon_tokutei_result2.xml") :: Nil

  override val outputFiles: Seq[DataFile[Row]] =
    CsvModel[Row](outputDir + TokuteiKenshinConst.Code
    ) :: CsvModel[Row](outputDir + TokuteiKenshinConst.PatientRole
    ) :: Nil

  var cached: RDD[(String, TokuteiKenshin)] = null

  override def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): Seq[DataFrame] = {
    // scala xmlで特定検診XMLのデータを操作
    import sparkSession.implicits._

    val tokuteiXmlStrs = inputs(0)
    val recordRDD = tokuteiXmlStrs.flatMap(xmlStr => {
      val xml = XML.loadString(xmlStr)
      TokuteiKenshinMapper.mapToTokuteiKennshinTuples(xml)
    })
    //何度も使用するのでキャッシュ
    cached = recordRDD.cache()

    //1つのRDDでXMLを各ファイルに分割するため別のDFに出力
    val codeDF = TokuteiKenshinMapper.extractRDD[Code](TokuteiKenshinConst.Code, cached).toDF()
    val patientDF = TokuteiKenshinMapper.extractRDD[PatientRole](TokuteiKenshinConst.PatientRole, cached).toDF()
    //TODO:レコード種別ごとに処理を追加していく
    codeDF :: patientDF :: Nil
  }

  override def tearDown(sparkSession: SparkSession): Unit = {
    //キャッシュの削除
    cached.unpersist()
    super.tearDown(sparkSession)
  }
}
