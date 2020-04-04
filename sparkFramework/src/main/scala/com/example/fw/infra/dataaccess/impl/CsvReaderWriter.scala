package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{Dataset, Encoder, Row, SaveMode, SparkSession}

//TODO:型パラーメータ化したい
class CsvReaderWriter extends DataFileReaderWriterImpl[Row] {
  override def read(inputFile: DataFile[Row], sparkSession: SparkSession): Dataset[Row] = {
    //import sparkSession.implicits._
    sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputFile.filePath)
    //TODO:as使えるようにしたい
      //.as[T]
  }

  override def write(ds: Dataset[Row], outputFile: DataFile[Row], saveMode: SaveMode): Unit = {
    ds.write
      .mode(saveMode)
      .csv(outputFile.filePath)
  }
}
