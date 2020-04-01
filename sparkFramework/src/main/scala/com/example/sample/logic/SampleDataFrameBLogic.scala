package com.example.sample.logic

import com.example.fw.domain.logic.DataFrameBLogic
import com.example.fw.domain.model.{JsonModel, ParquetModel}
import com.example.sample.model.Person
import org.apache.spark.sql.{DataFrame, SparkSession}

class SampleDataFrameBLogic extends DataFrameBLogic {
  //JSONファイルの読み込みの例
  override val inputFiles = JsonModel("C:\\temp\\person.json") :: Nil
  override val outputFiles = ParquetModel("C:\\temp\\person.parquet") :: Nil

  override def process(inputs: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame] = {
    //TODO: DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    import sparkSession.implicits._
    val ds = inputs(0).as[Person]
    ds.show()
    ds.toDF() :: Nil
  }
}
