package com.example.fw.domain.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.model.DataFile
import com.example.fw.infra.dataaccess.StandardSparkDataFileReaderWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

abstract class DataFrameBLogic() extends Logic {
  val inputFiles: Seq[DataFile[Row]]
  val outputFiles: Seq[DataFile[Row]]
  val dataFileReaderWriter: DataFileReaderWriter[Row]

  override final def execute(sparkSession: SparkSession): Unit = {
    setUp(sparkSession)
    val inputDatasets = input(sparkSession)
    val outputDatasets = process(inputDatasets, sparkSession)
    output(outputDatasets)
    tearDown(sparkSession)
  }

  def setUp(sparkSession: SparkSession): Unit = {
    logInfo("ビジネスロジック開始")
  }

  def input(sparkSession: SparkSession): Seq[DataFrame] = {
    inputFiles.map(
      inputFile => {
        dataFileReaderWriter.read(inputFile, sparkSession)
      }
    )
  }

  def process(inputs: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame]

  def output(outputs: Seq[DataFrame]): Unit = {
    outputs.zip(outputFiles).foreach(tuple => {
      val df = tuple._1
      val outputFile = tuple._2
      dataFileReaderWriter.write(df, outputFile)
    })
  }

  def tearDown(sparkSession: SparkSession): Unit = {
    logInfo("ビジネスロジック終了")
  }
}
