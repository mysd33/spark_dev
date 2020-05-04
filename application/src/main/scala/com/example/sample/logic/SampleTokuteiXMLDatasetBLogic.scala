package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataFile, XmlModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.example.fw.domain.utils.OptionImplicit._
import com.example.sample.common.tokutei.{Code, PatientRole}

import scala.collection.mutable

class SampleTokuteiXMLDatasetBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends DataFrameBLogic(dataFileReaderWriter) {
  override val inputFiles: Seq[DataFile[Row]] =
  //特定健診データを複数１つのXMLとして連結したもの
    XmlModel[Row](path = "tokutei/kensin_kihon_tokutei_result.xml",
      rowTag = "ClinicalDocument"
    ) :: Nil
  override val outputFiles: Seq[DataFile[Row]] =
    CsvModel[Row](
      "tokutei/output/code.csv"
    ) :: CsvModel[Row](
      "tokutei/output/patient_role.csv"
    ) :: Nil

  override def process(inputs: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame] = {
    import sparkSession.implicits._
    val df = inputs(0)

    //TODO: 特定検診の解析コンポーネントとして別クラスに切り出して単体テストしやすくする

    //TODO: spark-xmlは、ネストした複雑なXMLデータ構造だと煩雑なコードになってしまうし、ファイルを読んで逐次動作させながらでないと実装が難しい
    //報告区分(code)タグ
    val code = df.select("code")
      .map(row => {
        val codeTag = row.getAs[Row]("code")
        val code = codeTag.getAs[Long]("_code")
        val codeSystem = codeTag.getAs[String]("_codeSystem")
        Code(code, codeSystem)
      }).toDF()

    //受診者情報(recordTarget)タグ -> 患者情報を抽出
    val recordTarget = df.select("recordTarget")
    //recordTarget.printSchema()
    val patient = recordTarget.map(row => {
      val recordTargetTag = row.getAs[Row]("recordTarget")
      val patientRoleTag = recordTargetTag.getAs[Row]("patientRole")
      val idTags = patientRoleTag.getAs[mutable.Seq[Row]]("id")
      val hokenjaNo = idTags(0).getAs[Long]("_extension").toString
      // 全角数字が入っていたがLongに変換してちゃんと認識している模様。
      // ただ全て全角で入っているとStringで認識してしまう恐れあり。
      val hihokensaKigo = idTags(1).getAs[Long]("_extension").toString
      val hihokensaNo = idTags(2).getAs[Long]("_extension").toString
      PatientRole(hokenjaNo, hihokensaKigo, hihokensaNo)
    }).toDF()

    code :: patient :: Nil
  }

}
