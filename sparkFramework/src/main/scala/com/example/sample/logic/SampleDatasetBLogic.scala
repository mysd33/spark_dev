package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DatasetBLogic1
import com.example.fw.domain.model.{JsonModel, ParquetModel}
import com.example.sample.model.Person
import org.apache.spark.sql.Dataset

class SampleDatasetBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends DatasetBLogic1[Person, Person](dataFileReaderWriter) {

  override val inputFile = JsonModel[Person]("C:\\temp\\person.json")
  override val outputFile = ParquetModel[Person]("C:\\temp\\person.parquet")

  override def process(ds: Dataset[Person]): Dataset[Person] = {
    ds.show()
    ds
  }
}
