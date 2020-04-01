package com.example.fw.domain.logic

import com.example.sample.model.Person
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

abstract class DatasetBLogic extends Logic {
  //TODO:仮の記載
  //TODO: inputFile outputFileのリスト化
  val inputFiles: Seq[String]
  val outputFiles: Seq[String]

  override final def execute(sparkSession: SparkSession): Unit = {
    setUp(sparkSession)
    val inputDatasets = input(sparkSession)
    val outputDatasets = process(inputDatasets)
    output(outputDatasets)
    tearDown(sparkSession)
  }

  def setUp(sparkSession: SparkSession): Unit = {
  }

  //TODO: Datasetの型パラメータ化
  def input(sparkSession: SparkSession): Seq[Dataset[Person]] = {
    //TODO: DataReaderWriterに処理を切り出し
    import sparkSession.implicits._
    inputFiles.map(
      inputFile => {
        //TODO: 型パラメータ化
        sparkSession.read.json(inputFile).as[Person]
      }
    )
  }

  //TODO: Datasetの型パラメータのパラメータ化
  def process(inputs: Seq[Dataset[Person]]): Seq[Dataset[Person]]

  //TODO: Datasetの型パラメータのパラメータ化
  def output(outputs: Seq[Dataset[Person]]): Unit = {
    //TODO: DataReaderWriterに処理を切り出し
    //TODO: 複数のDatasetの繰り返し処理に対応
    outputs.zip(outputFiles).foreach(tuple => {
      val ds = tuple._1
      val outputFile = tuple._2
      ds.write.mode(SaveMode.Overwrite).parquet(outputFile)
    })
  }

  def tearDown(sparkSession: SparkSession): Unit = {
  }
}
