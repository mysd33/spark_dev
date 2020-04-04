package com.example.fw.domain.dataaccess

import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

trait DataFileReaderWriterImpl[T] {
  def read(inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T]
  def write(ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode = SaveMode.Overwrite): Unit
}
