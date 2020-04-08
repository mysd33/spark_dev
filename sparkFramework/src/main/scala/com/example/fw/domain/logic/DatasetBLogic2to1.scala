package com.example.fw.domain.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

abstract class DatasetBLogic2to1[T1 <: Product : TypeTag, T2 <: Product : TypeTag, U <: Product : TypeTag]
(val dataFileReaderWriter: DataFileReaderWriter) extends Logic {
  val inputFile1: DataFile[T1]
  val inputFile2: DataFile[T2]
  val outputFile: DataFile[U]

  override final def execute(sparkSession: SparkSession): Unit = {
    setUp(sparkSession)
    val inputDatasets = input(sparkSession)
    val outputDatasets = process(inputDatasets._1, inputDatasets._2, sparkSession)
    output(outputDatasets)
    tearDown(sparkSession)
  }

  def setUp(sparkSession: SparkSession): Unit = {
  }

  final def input(sparkSession: SparkSession): Tuple2[Dataset[T1], Dataset[T2]] = {
    val input1 = dataFileReaderWriter.readToDs(inputFile1, sparkSession)
    val input2 = dataFileReaderWriter.readToDs(inputFile2, sparkSession)
    (input1, input2)
  }

  def process(ds1: Dataset[T1], ds2: Dataset[T2], sparkSession: SparkSession): Dataset[U]

  final def output(ds: Dataset[U]): Unit = {
    dataFileReaderWriter.writeFromDs(ds, outputFile)
  }

  def tearDown(sparkSession: SparkSession): Unit = {
  }

}
