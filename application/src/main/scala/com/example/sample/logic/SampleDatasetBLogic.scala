package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DatasetBLogic1to1
import com.example.fw.domain.model.{JsonModel, ParquetModel}
import com.example.sample.model.Person
import org.apache.spark.sql.{Dataset, SparkSession}

class SampleDatasetBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends DatasetBLogic1to1[Person, Person](dataFileReaderWriter) {

  override val inputFile = JsonModel[Person]("person.json")
  override val outputFile = ParquetModel[Person]("person.parquet")

  override def process(ds: Dataset[Person], sparkSession: SparkSession): Dataset[Person] = {
    //DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    import sparkSession.implicits._
    ds.show()
    ds
  }
}
