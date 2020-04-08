package com.example.fw.domain.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.reflect.runtime.universe.TypeTag

abstract class DatasetBLogic1[T <: Product : TypeTag, U <: Product :TypeTag](val dataFileReaderWriter: DataFileReaderWriter) extends Logic {
  val inputFile: DataFile[T]
  val outputFile: DataFile[U]

  override final def execute(sparkSession: SparkSession): Unit = {
    setUp(sparkSession)
    val inputDatasets = input(sparkSession)
    val outputDatasets = process(inputDatasets)
    output(outputDatasets)
    tearDown(sparkSession)
  }

  def setUp(sparkSession: SparkSession): Unit = {
  }

  def input(sparkSession: SparkSession): Dataset[T] = {
    dataFileReaderWriter.readToDs(inputFile, sparkSession)
  }

  def process(ds: Dataset[T]): Dataset[U]

  def output(ds: Dataset[U]): Unit = {
    dataFileReaderWriter.writeFromDs(ds, outputFile)
  }

  def tearDown(sparkSession: SparkSession): Unit = {
  }
}
