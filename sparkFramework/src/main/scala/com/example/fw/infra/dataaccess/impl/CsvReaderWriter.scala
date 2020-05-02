package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import scala.reflect.runtime.universe.TypeTag

class CsvReaderWriter extends DataFileReaderWriterImpl {
  override def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    val reader = sparkSession.read
    inputFile.schema match {
      case Some(schm) => reader
        .schema(schm)
        .csv(inputFile.filePath)
      case _ => reader
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputFile.filePath)
    }
  }

  override def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    val reader = sparkSession.read
    inputFile.schema match {
      case Some(schm) => reader
        .schema(schm)
        .csv(inputFile.filePath).as[T]
      case _ => reader
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputFile.filePath).as[T]
    }
  }

  override def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode): Unit = {
    val writer =
      if (outputFile.hasPartition) {
        ds.write.mode(saveMode).partitionBy(outputFile.partition.get)
      } else {
        ds.write.mode(saveMode)
      }
    writer.csv(outputFile.filePath)
  }
}
