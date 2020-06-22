package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.logic.DatasetBLogic1to1
import com.example.fw.domain.model.{JsonModel, ParquetModel}
import com.example.sample.common.entity.Person
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * AP基盤を使ったサンプル
 *
 * DatasetBLogic1to1クラスを継承し、Jsonファイルを読み込んでParquetファイルを出力するサンプル
 *
 * @param dataModelReaderWriter Logicクラスが使用するDataModelReaderWriter
 */
class SampleDatasetBLogic(dataModelReaderWriter: DataModelReaderWriter)
  extends DatasetBLogic1to1[Person, Person](dataModelReaderWriter) {

  //Jsonファイル読み込みの例。Modelの型パラメータをcaseクラス（エンティティクラス）で定義。
  override val inputModel = JsonModel[Person]("person.json")
  //Parquetファイル読み込みの例。Modelの型パラメータをcaseクラス（エンティティクラス）で定義。
  override val outputModel = ParquetModel[Person]("person.parquet")

  override def process(ds: Dataset[Person], sparkSession: SparkSession): Dataset[Person] = {
    //DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    import sparkSession.implicits._
    ds.show()
    ds
  }
}
