package com.example.fw.domain.dataaccess

import com.example.fw.domain.model.DataFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait DataFileReaderWriterImpl {
  def readToRDD(inputFile: DataFile[String], sparkSession: SparkSession): RDD[String] = ???
  def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = ???
  def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = ???
  def writeFromRDD[T](rdd: RDD[T], outputFile: DataFile[T]): Unit = ???
  def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode = SaveMode.Overwrite): Unit = ???
}
