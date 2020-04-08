package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DatasetBLogic1to1
import com.example.fw.domain.model.{CsvModel, DataFile, ParquetModel}
import com.example.sample.model.Person
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

class SampleDataSetBLogic3(dataFileReaderWriter: DataFileReaderWriter)
  extends DatasetBLogic1to1[Person, Person](dataFileReaderWriter) {
  override val inputFile: DataFile[Person] = CsvModel[Person](
    "person_noheader.csv",
    StructType(Array(
      StructField("age", LongType, true),
      StructField("name", StringType, true)
    ))
  )
  override val outputFile: DataFile[Person] = ParquetModel[Person]("person.parquet")

  override def process(ds: Dataset[Person], sparkSession: SparkSession): Dataset[Person] = {
    //DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    ds.show()
    ds
  }
}
