package com.example.fw.domain.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.model.DataFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.reflect.runtime.universe.TypeTag

abstract class RDDToDatasetBLogic[U <: Product : TypeTag](val dataFileReaderWriter: DataFileReaderWriter) extends Logic {
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

  def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): Dataset[U]

  final def output(ds: Dataset[U]): Unit = {
    dataFileReaderWriter.writeFromDs(ds, outputFile)
  }

  def tearDown(sparkSession: SparkSession): Unit = {
    logInfo("ビジネスロジック終了")
  }
}
