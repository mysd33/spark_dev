package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

//TODO: Databricks依存のFWプロジェクトに切り出す
class DeltaLakeReaderWriter extends DataFileReaderWriterImpl[Row] {
  val formatName = "delta"

  //TODO:型パラーメータ化したい
  override def read(inputFile: DataFile[Row], sparkSession: SparkSession): Dataset[Row] = {
    //import sparkSession.implicits._
    sparkSession.read
      .format(formatName)
      .load(inputFile.filePath)
    //TODO: as使いたい
    //.as[T]
  }

  override def write(ds: Dataset[Row], outputFile: DataFile[Row], saveMode: SaveMode): Unit = {
    ds.write
      .mode(saveMode)
      .format(formatName)
      .save(outputFile.filePath)
  }
}
