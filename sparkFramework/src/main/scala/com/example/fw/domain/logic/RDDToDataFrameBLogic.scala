package com.example.fw.domain.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.model.DataFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

abstract class RDDToDataFrameBLogic (val dataFileReaderWriter: DataFileReaderWriter) extends Logic {
  val inputFiles: Seq[DataFile[String]]
  val outputFiles: Seq[DataFile[Row]]

  override final def execute(sparkSession: SparkSession): Unit = {
    try {
      setUp(sparkSession)
      val inputDatasets = input(sparkSession)
      val outputDatasets = process(inputDatasets, sparkSession)
      output(outputDatasets)
    } finally {
      tearDown(sparkSession)
    }
  }

  def setUp(sparkSession: SparkSession): Unit = {
    logInfo("ビジネスロジック開始:" + getClass().getTypeName())
  }

  final def input(sparkSession: SparkSession): Seq[RDD[String]] = {
    inputFiles.map(
      inputFile => {
        dataFileReaderWriter.readToRDD(inputFile, sparkSession)
      }
    )
  }

  def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): Seq[DataFrame]

  final def output(outputs: Seq[DataFrame]): Unit = {
    outputs.zip(outputFiles).foreach(tuple => {
      val df = tuple._1
      val outputFile = tuple._2
      dataFileReaderWriter.writeFromDf(df, outputFile)
    })
  }

  def tearDown(sparkSession: SparkSession): Unit = {
    logInfo("ビジネスロジック終了")
  }
}