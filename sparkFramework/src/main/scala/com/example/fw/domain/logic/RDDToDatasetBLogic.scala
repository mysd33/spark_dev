package com.example.fw.domain.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.model.DataFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

abstract class RDDToDatasetBLogic[U](val dataFileReaderWriter: DataFileReaderWriter) extends Logic {
  val inputFiles: Seq[DataFile[String]]
  val outputFile: DataFile[U]


  override final def execute(sparkSession: SparkSession): Unit = {
    try {
      setUp(sparkSession)
      val inputDatasets = input(sparkSession)
      val outputDataset = process(inputDatasets, sparkSession)
      output(outputDataset)
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

  def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): RDD[U]

  final def output(rdd: RDD[U]): Unit = {
    dataFileReaderWriter.writeFromRDD(rdd, outputFile)
  }

  def tearDown(sparkSession: SparkSession): Unit = {
    logInfo("ビジネスロジック終了")
  }
}
