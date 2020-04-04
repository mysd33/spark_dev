package com.example.fw.domain.dataaccess

import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

class DataFileReaderWriter[T] {
  //自分型アノテーションでDataFileReaderWriterの実装をDI
  self: DataFileReaderWriterImpl[T] =>
  def read(inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    self.read(inputFile, sparkSession)
  }

  def write(ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    self.write(ds, outputFile, saveMode)
  }

}
