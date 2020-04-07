package com.example.fw.domain.dataaccess

import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait DataFileReaderWriterImpl {
  //TODO:本当は型パラメータを工夫して1つにしたいができるか？
  def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T]
  def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame
  //TODO:メソッド名
  def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode = SaveMode.Overwrite): Unit
}
