package com.example.fw.infra.dataaccess

import com.example.fw.domain.model.{DataFile, ParquetModel}
import com.example.fw.infra.dataaccess.impl.DeltaLakeReaderWriter
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe

trait DatabricksDataFileReaderWriter extends StandardSparkDataFileReaderWriter {
  override def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    inputFile match {
      //DeltaLakeのみReaderWriterを差し替え
      case f: ParquetModel[Row] => new DeltaLakeReaderWriter().readToDf(f, sparkSession)
      case _ => super.readToDf(inputFile, sparkSession)
    }
  }

  override def readToDs[T <: Product : universe.TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    inputFile match {
      //DeltaLakeのみReaderWriterを差し替え
      case f: ParquetModel[T] => new DeltaLakeReaderWriter().readToDs(f, sparkSession)
      case _ => super.readToDs(inputFile, sparkSession)
    }
  }

  override def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode): Unit = {
    outputFile match {
      //DeltaLakeのみReaderWriterを差し替え
      case f: ParquetModel[T] => new DeltaLakeReaderWriter().writeFromDsDf(ds, f, saveMode)
      case _ => super.writeFromDsDf(ds, outputFile, saveMode)
    }
  }
}
