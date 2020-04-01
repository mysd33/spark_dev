package com.example.sample.logic

import com.example.fw.domain.logic.DatasetBLogic
import com.example.sample.model.Person
import org.apache.spark.sql.Dataset

class SampleDatasetBLogic extends DatasetBLogic {
  //TODO:仮の記載
  override val inputFiles = "C:\\temp\\person.json" :: Nil
  override val outputFiles = "C:\\temp\\person.parquet" :: Nil

  override def process(inputs: Seq[Dataset[Person]]): Seq[Dataset[Person]] = {
    val ds = inputs(0)
    ds.show()
    ds :: Nil
  }
}
