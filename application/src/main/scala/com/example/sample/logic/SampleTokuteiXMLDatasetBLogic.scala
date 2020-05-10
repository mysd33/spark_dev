package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataFile, XmlModel}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.example.fw.domain.utils.OptionImplicit._
import com.example.sample.common.tokutei.{Code, PatientRole, TokuteiKenshinConst}

import scala.collection.mutable

//TODO: spark-xmlは、ネストした複雑なXMLデータ構造だと煩雑なコードになってしまうし、ファイルを読んで逐次動作させながらでないと実装が難しいので使わない
/**
 * AP基盤を使ったサンプル
 *
 * 事前にシェルで、1行1特定検診XMLで複数特定検診のXMLを連結したテキストファイルを
 * 読み込み、各タグごとに、CSVファイルに書き込みを試行した例
 *
 * spark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しないので注意。
 *
 * @deprecated spark-xmlは、ネストした複雑なXMLデータ構造だと煩雑なコードになってしまうし、ファイルを読んで逐次動作させながらでないと実装が難しいので使わない
 *
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 */
class SampleTokuteiXMLDatasetBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends DataFrameBLogic(dataFileReaderWriter) {
  private val outputDir = "tokutei/output/"

  //特定健診データを複数１つのXMLとして連結したもの
  override val inputFiles: Seq[DataFile[Row]] =
    XmlModel[Row](relativePath = "tokutei/kensin_kihon_tokutei_result.xml",
      rowTag = "ClinicalDocument"
    ) :: Nil

  override val outputFiles: Seq[DataFile[Row]] =
    CsvModel[Row](outputDir + TokuteiKenshinConst.Code, compression = "bzip2"
    ) :: CsvModel[Row](outputDir + TokuteiKenshinConst.PatientRole, compression = "bzip2"
    ) :: Nil

  override def process(inputs: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame] = {
    import sparkSession.implicits._
    val df = inputs(0)

    //本来なら特定検診の解析コンポーネントとして別クラスに切り出して単体テストしやすくする
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
