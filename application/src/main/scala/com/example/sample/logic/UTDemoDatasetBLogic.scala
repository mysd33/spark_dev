package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.logic.DatasetBLogic1to1
import com.example.fw.domain.message.Message
import com.example.fw.domain.model.{CsvModel, DataModel, ParquetModel}
import com.example.sample.common.logic.SampleSharedLogic
import com.example.sample.common.rule.PersonRule
import com.example.sample.common.entity.Person
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import com.example.fw.domain.utils.OptionImplicit._
import com.example.sample.common.udf.PersonRuleUDF


/**
 * AP基盤を使ったサンプル
 *
 * 単体テストコードデモ用に作成
 * @see com.example.sample.logic.UTDemoDatasetBLogicTest
 *
 * @param dataModelReaderWriter Logicクラスが使用するDataModelReaderWriter
 */
class UTDemoDatasetBLogic(dataModelReaderWriter: DataModelReaderWriter) extends
  DatasetBLogic1to1[Person, Person](dataModelReaderWriter) {

  //共通処理クラス
  val sampleSharedLogic: SampleSharedLogic = new SampleSharedLogic(dataModelReaderWriter)

  //ビジネスルールクラス
  val personRule = new PersonRule

  override val inputModel: DataModel[Person] = CsvModel[Person](
    "person_noheader.csv",
    schema = StructType(Array(
      StructField("age", LongType, true),
      StructField("name", StringType, true)
    ))
  )
  override val outputModel: DataModel[Person] = ParquetModel[Person]("person.parquet")

  override def process(ds: Dataset[Person], sparkSession: SparkSession): Dataset[Person] = {
    import sparkSession.implicits._
    //共通処理の呼び出し
    val tempDs = sampleSharedLogic.execute(ds)
    //NotSerializableException(Task not Serialzable)を回避するため、いったんビジネスルールオブジェクトをローカル変数に格納
    val rule = personRule
    val result = tempDs.map(p => {
      //ビジネスルールの呼び出し
      val age = rule.calcAge(p)
      Person("hoge", Some(age))
    })
    result.show()

    //UDFサンプル
    import org.apache.spark.sql.functions.col
    //UDFの取得
    val calcAge = PersonRuleUDF.calcAge
    val ageDs =
      Seq("20200101", "20100101", "200001010").toDF("value")
      .select(calcAge(col("value")) as "age")
    ageDs.show()

    //メッセージ
    logInfo(Message.get("i.xxx.001", "hoge"))
    logWarning(Message.get("w.xxx.001", "fuga"))
    logError(Message.get("e.xxx.001", "foo"))

    ds
  }

}
