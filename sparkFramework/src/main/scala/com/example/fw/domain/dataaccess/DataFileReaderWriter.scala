package com.example.fw.domain.dataaccess

import com.example.fw.domain.model.DataFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

class DataFileReaderWriter extends Serializable {
  //自分型アノテーションでDataFileReaderWriterの実装をDI
  self: DataFileReaderWriterImpl =>
  def readToRDD(inputFile: DataFile[String], sparkSession: SparkSession): RDD[String] = {
    self.readToRDD(inputFile, sparkSession)
  }

  def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    self.readToDs(inputFile, sparkSession)
  }

  def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    self.readToDf(inputFile, sparkSession)
  }

  def writeFromRDD[T](rdd: RDD[T], outputFile: DataFile[T]): Unit = {
    self.writeFromRDD(rdd, outputFile)
  }

  def writeFromDf[T](ds: DataFrame, outputFile: DataFile[Row], saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    self.writeFromDsDf(ds, outputFile, saveMode)
  }

  def writeFromDs[T <: Product : TypeTag](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    self.writeFromDsDf(ds, outputFile, saveMode)
  }
}
