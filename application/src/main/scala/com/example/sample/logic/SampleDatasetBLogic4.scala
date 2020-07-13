package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.logic.DatasetBLogic1to1
import com.example.fw.domain.model.{CsvModel, DataModel, DwDmModel, ParquetModel}
import com.example.fw.domain.utils.OptionImplicit._
import com.example.sample.common.entity.Person
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * AP基盤を使ったサンプル
 *
 * DatasetBLogic1to1クラスを継承し、ヘッダなしCsvファイルを読み込んでSynapseAnalyticsのテーブルへ出力するサンプル
 *
 * @param dataModelReaderWriter Logicクラスが使用するDataModelReaderWriter
 */
class SampleDatasetBLogic4(dataModelReaderWriter: DataModelReaderWriter)
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
  //Synapse AnalyticsのPersonテーブルへの書き込みの例
  override val outputModel: DataModel[Person] = DwDmModel[Person](
    dbTable = "Person"
  )

  override def process(ds: Dataset[Person], sparkSession: SparkSession): Dataset[Person] = {
    //DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    ds.show()
    ds
  }
}
