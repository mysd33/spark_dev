package com.example.fw.infra.dataaccess

import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object DataFileReaderWriter {
  def read[T](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    val reader = sparkSession.read
    inputFile.read(reader)
  }

  def write[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    val writer = ds.write.mode(saveMode)
    outputFile.write(writer)
  }
}
