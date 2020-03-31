package com.example.samle.businesslogic

import com.example.fw.DataFrameBLogic
import com.example.samle.model.Person
import org.apache.spark.sql.{DataFrame, SparkSession}

class SampleDataFrameBLogic extends DataFrameBLogic {
  //TODO:仮の記載
  override val inputFiles = "C:\\temp\\person.json" :: Nil
  override val outputFiles = "C:\\temp\\person.parquet" :: Nil

  override def process(inputs: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame] = {
    import sparkSession.implicits._
    val ds = inputs(0).as[Person]
    ds.show()
    ds.toDF() :: Nil
  }
}
