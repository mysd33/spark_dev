package com.example.fw.domain.dataaccess

import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import scala.reflect.runtime.universe.TypeTag

class DataFileReaderWriter {
  //自分型アノテーションでDataFileReaderWriterの実装をDI
  self: DataFileReaderWriterImpl =>
  //TODO:本当は型パラメータを工夫して1つにしたいができるか？
  def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    self.readToDs(inputFile, sparkSession)
  }

  def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    self.readToDf(inputFile, sparkSession)
  }

  //TODO:メソッド名
  def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    self.writeFromDsDf(ds, outputFile, saveMode)
  }
}
