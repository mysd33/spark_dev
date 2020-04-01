package com.example.fw

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

abstract class DataFrameBLogic extends Logic {
  //TODO:仮の記載
  //TODO: inputFile outputFileのリスト化
  val inputFiles: Seq[String]
  val outputFiles: Seq[String]

  override def execute(sparkSession: SparkSession): Unit = {
    setUp(sparkSession)
    val inputDatasets = input(sparkSession)
    val outputDatasets = process(inputDatasets, sparkSession)
    output(outputDatasets)
    tearDown(sparkSession)
  }

  def setUp(sparkSession: SparkSession): Unit = {
  }

  def input(sparkSession: SparkSession): Seq[DataFrame] = {
    inputFiles.map(
      inputFile => {
        DataFrameReaderWriter.read(sparkSession, inputFile)
      }
    )
  }

  def process(inputs: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame]

  def output(outputs: Seq[DataFrame]): Unit = {
    outputs.zip(outputFiles).foreach(tuple => {
      val df = tuple._1
      val outputFile = tuple._2
      DataFrameReaderWriter.write(df, outputFile)
    })
  }

  def tearDown(sparkSession: SparkSession): Unit = {
  }
}
