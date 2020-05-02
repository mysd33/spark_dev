package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DatasetBLogic1to1
import com.example.fw.domain.model.{CsvModel, DataFile, ParquetModel}
import com.example.sample.common.logic.SampleSharedLogic
import com.example.sample.common.rule.PersonRule
import com.example.sample.model.Person
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

//単体テストコードデモ用
class UTDemoDatasetBLogic(dataFileReaderWriter: DataFileReaderWriter) extends
  DatasetBLogic1to1[Person, Person](dataFileReaderWriter) {

  //共通処理クラス
  val sampleSharedLogic: SampleSharedLogic = new SampleSharedLogic(dataFileReaderWriter)

  //ビジネスルールクラス
  val personRule = new PersonRule

  override val inputFile: DataFile[Person] = CsvModel[Person](
    "person_noheader.csv",
    schm = StructType(Array(
      StructField("age", LongType, true),
      StructField("name", StringType, true)
    ))
  )
  override val outputFile: DataFile[Person] = ParquetModel[Person]("person.parquet")

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
    ds
  }

}
