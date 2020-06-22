package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.RDDToDataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataModel, TextLineModel}
import com.example.sample.common.tokutei.{Code, CodeTokuteiKenshinMapper, PatientRole, PatientRoleTokuteiKenshinMapper, TokuteiKenshin, TokuteiKenshinConst, TokuteiKenshinMapper}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.xml.XML

import com.example.fw.domain.utils.OptionImplicit._

/**
 * AP基盤を使ったサンプル
 *
 * 事前にシェルで、1行1特定検診XMLで複数特定検診のXMLを連結したテキストファイルを
 * 読み込み、各タグごとに、CSVファイルに書き込みを試行した例
 *
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 */
class SampleTokuteiXMLDataFrameBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends RDDToDataFrameBLogic(dataFileReaderWriter) {

  //1行1特定検診XMLで複数特定検診のXMLを連結したテキストファイルを読み込む例。TextLineModelで扱う
  override val inputFiles: Seq[DataModel[String]] =
    TextLineModel[String]("tokutei/kensin_kihon_tokutei_result2.xml") :: Nil

  //CSVファイルに書き込む例。bzip2でファイル圧縮
  private val outputDir = "tokutei/output2/"
  override val outputFiles: Seq[DataModel[Row]] =
    CsvModel[Row](outputDir + TokuteiKenshinConst.Code, compression = "bzip2"
    ) :: CsvModel[Row](outputDir + TokuteiKenshinConst.PatientRole, compression = "bzip2"
    ) :: Nil

  private var cached: RDD[(String, TokuteiKenshin)] = null

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
