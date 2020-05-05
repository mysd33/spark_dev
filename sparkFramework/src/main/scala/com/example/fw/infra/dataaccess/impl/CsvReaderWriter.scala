package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.{CsvModel, DataFile}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

class CsvReaderWriter {
  //TODO:csvのoptionの実装
  //http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader@csv(paths:String*):org.apache.spark.sql.DataFrame
  //http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter@csv(path:String):Unit

  def readToDf(inputFile: CsvModel[Row], sparkSession: SparkSession): DataFrame = {
    val reader = sparkSession.read
    val reader2 = inputFile.schema match {
      case Some(schm) => reader.schema(schm)
      case _ => reader
        //TODO: DataFileで設定可能にする
        .option("header", "true")
        .option("inferSchema", "true")
    }
    val reader3 = inputFile.encoding match {
      case Some(encoding) => reader2.option("encoding", encoding)
      case _ => reader2
    }
    reader3.csv(inputFile.filePath)
  }

  def readToDs[T <: Product : TypeTag](inputFile: CsvModel[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    val reader = sparkSession.read
    val reader2 = inputFile.schema match {
      case Some(schm) => reader.schema(schm)
      case _ => reader
        //TODO: DataFileで設定可能にする
        .option("header", "true")
        .option("inferSchema", "true")
    }
    val reader3 = inputFile.encoding match {
      case Some(encoding) => reader2.option("encoding", encoding)
      case _ => reader2
    }
    reader3.csv(inputFile.filePath).as[T]
  }

  def writeFromDsDf[T](ds: Dataset[T], outputFile: CsvModel[T], saveMode: SaveMode): Unit = {
    val writer = ds.write.mode(saveMode)
      //TODO: DataFileで設定可能にする
      .option("header", "true")
      //TODO: DataFileで設定可能にする
      //.option("compression", "bzip2")
    val writer2 = outputFile.partition match {
      case Some(partition) => writer.partitionBy(partition)
      case _ => writer
    }
    val writer3 = outputFile.encoding match {
      case Some(encoding) => writer2.option("encoding", encoding)
      case _ => writer2
    }
    writer3.csv(outputFile.filePath)
  }

}
