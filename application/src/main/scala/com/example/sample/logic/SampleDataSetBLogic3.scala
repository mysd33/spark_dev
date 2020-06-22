package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.logic.DatasetBLogic1to1
import com.example.fw.domain.model.{CsvModel, DataModel, ParquetModel}
import com.example.sample.common.entity.Person
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

import com.example.fw.domain.utils.OptionImplicit._

/**
 * AP基盤を使ったサンプル
 *
 * DatasetBLogic1to1クラスを継承し、ヘッダなしCsvファイルを読み込んでParquetファイルを出力するサンプル
 *
 * @param dataModelReaderWriter Logicクラスが使用するDataModelReaderWriter
 */
//TODO:クラス名をSampleDataSetBLogic3ではなくSampleDatasetBLogic3(setのsが小文字に変更）
class SampleDataSetBLogic3(dataModelReaderWriter: DataModelReaderWriter)
  extends DatasetBLogic1to1[Person, Person](dataModelReaderWriter) {
  //ヘッダなしのCSVファイルの読み込みの例
  override val inputModel: DataModel[Person] = CsvModel[Person](
    "person_noheader.csv",
    //caseクラス（Person）とマッピングさせるようスキーマ定義する
    schema = StructType(Array(
      StructField("age", LongType, true),     //1列目
      StructField("name", StringType, true)   //2列目
    ))
  )
  //Parquetファイルの書き込みの例
  override val outputModel: DataModel[Person] = ParquetModel[Person]("person.parquet")

  override def process(ds: Dataset[Person], sparkSession: SparkSession): Dataset[Person] = {
    //DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    ds.show()
    ds
  }
}
