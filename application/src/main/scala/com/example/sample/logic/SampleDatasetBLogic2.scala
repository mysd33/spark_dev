package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DatasetBLogic2to1
import com.example.fw.domain.model.{DataFile, JsonModel, ParquetModel}
import com.example.sample.common.entity.{Person, PersonOther}
import org.apache.spark.sql.{Dataset, SparkSession}

class SampleDatasetBLogic2(dataFileReaderWriter: DataFileReaderWriter)
  extends DatasetBLogic2to1[Person, PersonOther, Person](dataFileReaderWriter) {

  override val inputFile1: DataFile[Person] = JsonModel[Person]("person.json")
  override val inputFile2: DataFile[PersonOther] = JsonModel[PersonOther]("person.json")
  override val outputFile: DataFile[Person] = ParquetModel[Person]("person_union.parquet")

  override def process(input1: Dataset[Person], input2: Dataset[PersonOther], sparkSession: SparkSession): Dataset[Person] = {
    import sparkSession.implicits._
    //DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    val dsTemp = input2.map(po => Person(po.name, po.age))
    //unionの例
    val ds3 = input1.unionByName(dsTemp)
    ds3.show()
    ds3
  }
}
