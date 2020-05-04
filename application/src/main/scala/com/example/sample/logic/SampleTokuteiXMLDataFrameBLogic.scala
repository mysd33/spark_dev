package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.RDDToDataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataFile, TextFileModel}
import com.example.sample.common.tokutei.{Code, PatientRole, TokuteiKenshin}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.xml.XML

class SampleTokuteiXMLDataFrameBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends RDDToDataFrameBLogic(dataFileReaderWriter) {
  //1行1特定検診XMLのテキストファイルとして扱う
  override val inputFiles: Seq[DataFile[String]] =
    TextFileModel[String]("tokutei/kensin_kihon_tokutei_result2.xml") :: Nil

  override val outputFiles: Seq[DataFile[Row]] =
    CsvModel[Row](
      "tokutei/output2/code.csv"
    ) :: CsvModel[Row](
      "tokutei/output2/patient_role.csv"
    ) :: Nil

  var cached: RDD[(String, TokuteiKenshin)] = null

  override def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): Seq[DataFrame] = {
    // scala xmlで特定検診XMLのデータを操作
    import sparkSession.implicits._

    val tokuteiXmlStrs = inputs(0)
    val recordRDD = tokuteiXmlStrs.flatMap(xmlStr => {
      val xml = XML.loadString(xmlStr)
      //TODO: 別々のMapperコードに切り出す
      val codeTag = xml \ "code"
      val codeValue = codeTag \@ "code"
      val codeSystem = codeTag \@ "codeSystem"
      val code: TokuteiKenshin = Code(codeValue.toLong, codeSystem)

      val patientRoleTag = xml \ "recordTarget" \ "patientRole"
      val idTags = patientRoleTag \\ "id"
      val hokenjaNo = idTags(0) \@ "extension"
      val hihokensaKigo = idTags(1) \@ "extension"
      val hihokensaNo = idTags(2) \@ "extension"
      val patientRole: TokuteiKenshin = PatientRole(hokenjaNo, hihokensaKigo, hihokensaNo)
      ("Code", code) :: ("patientRole", patientRole) :: Nil
    })
    //何度も使用するのでキャッシュ
    cached = recordRDD.cache()

    //1つのRDDでXMLを各ファイルに分割するため別のDFに出力
    val codeDF = cached.filter(t => t._1 == "Code")
      .map(t => t._2.asInstanceOf[Code]).toDF()
    val patientDF = cached.filter(t => t._1 == "patientRole")
      .map(t => t._2.asInstanceOf[PatientRole]).toDF()

    codeDF :: patientDF :: Nil
  }

  override def tearDown(sparkSession: SparkSession): Unit = {
    //キャッシュの削除
    cached.unpersist()
    super.tearDown(sparkSession)
  }

}
