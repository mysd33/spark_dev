package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DataFrameBLogic
import com.example.fw.domain.model.{CsvModel, ParquetModel}
import com.example.sample.model.Person
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class SampleDataFrameBLogic2(dataFileReaderWriter: DataFileReaderWriter[Row])
  extends DataFrameBLogic(dataFileReaderWriter) {
  //CSVファイルの読み込みの例
  override val inputFiles = CsvModel("C:\\temp\\person.csv") :: Nil
  override val outputFiles = ParquetModel("C:\\temp\\person.parquet") :: Nil

  override def process(inputs: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame] = {
    //TODO: DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    import sparkSession.implicits._
    val ds = inputs(0).as[Person]
    ds.show()
    ds.toDF() :: Nil
  }
}
