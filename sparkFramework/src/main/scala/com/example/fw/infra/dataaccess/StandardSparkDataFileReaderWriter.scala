package com.example.fw.infra.dataaccess

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.{CsvModel, DataFile, JsonModel, MultiFormatCsvModel, ParquetModel}
import com.example.fw.infra.dataaccess.impl.{CsvReaderWriter, JsonReaderWriter, StandardParquetReaderWriter}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait StandardSparkDataFileReaderWriter extends DataFileReaderWriterImpl {
  override def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    inputFile match {
      case f: CsvModel[Row] => new CsvReaderWriter().readToDf(f, sparkSession)
      case f: JsonModel[Row] => new JsonReaderWriter().readToDf(f, sparkSession)
      case f: ParquetModel[Row] => new StandardParquetReaderWriter().readToDf(f, sparkSession)
      //TODO: MultiFormatCsvModel, XmlModel
    }
  }

  override def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    inputFile match {
      case f: CsvModel[T] => new CsvReaderWriter().readToDs(f, sparkSession)
      case f: JsonModel[T] => new JsonReaderWriter().readToDs(f, sparkSession)
      case f: ParquetModel[T] => new StandardParquetReaderWriter().readToDs(f, sparkSession)
      //TODO: MultiFormatCsvModel, XmlModel
    }
  }

  override def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode): Unit = {
    outputFile match {
      case f: CsvModel[T] => new CsvReaderWriter().writeFromDsDf(ds, f, saveMode)
      case f: JsonModel[T] => new JsonReaderWriter().writeFromDsDf(ds, f, saveMode)
      case f: ParquetModel[T] => new StandardParquetReaderWriter().writeFromDsDf(ds, f, saveMode)
      //TODO: MultiFormatCsvModel, XmlModel
    }
  }
}