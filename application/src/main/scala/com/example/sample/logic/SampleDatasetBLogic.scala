package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DatasetBLogic1to1
import com.example.fw.domain.model.{JsonModel, ParquetModel}
import com.example.sample.common.entity.Person
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * AP基盤を使ったサンプル
 *
 * DataSetBLogic1to1クラスを継承し、Jsonファイルを読み込んでParquetファイルを出力するサンプル
 *
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 */
class SampleDatasetBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends DatasetBLogic1to1[Person, Person](dataFileReaderWriter) {

  //Jsonファイル読み込みの例。Modelの型パラメータをcaseクラス（エンティティクラス）で定義。
  override val inputFile = JsonModel[Person]("person.json")
  //Parquetファイル読み込みの例。Modelの型パラメータをcaseクラス（エンティティクラス）で定義。
  override val outputFile = ParquetModel[Person]("person.parquet")

  override def process(input: Dataset[Person], sparkSession: SparkSession): Dataset[Person] = {
    //DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    import sparkSession.implicits._
    input.show()
    input
  }
}
