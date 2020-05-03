package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import scala.reflect.runtime.universe.TypeTag

class CsvReaderWriter extends DataFileReaderWriterImpl {
  override def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    val reader = sparkSession.read
    val reader2 = inputFile.schema match {
      case Some(schm) => reader.schema(schm)
      case _ => reader
        .option("header", "true")
        .option("inferSchema", "true")
    }
    val reader3 = inputFile.encoding match {
      case Some(encoding) => reader2.option("encoding", encoding)
      case _ => reader2
    }
    reader3.csv(inputFile.filePath)
  }

  override def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    val reader = sparkSession.read
    val reader2 = inputFile.schema match {
      case Some(schm) => reader.schema(schm)
      case _ => reader
        .option("header", "true")
        .option("inferSchema", "true")
    }
    val reader3 = inputFile.encoding match {
      case Some(encoding) => reader2.option("encoding", encoding)
      case _ => reader2
    }
    reader3.csv(inputFile.filePath).as[T]
  }

  override def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode): Unit = {
    val writer = ds.write.mode(saveMode)
      //TODO: DataFileで設定可能にする
      .option("header", "true")
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
