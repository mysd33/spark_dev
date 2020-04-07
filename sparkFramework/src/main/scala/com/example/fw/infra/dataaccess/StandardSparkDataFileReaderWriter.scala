package com.example.fw.infra.dataaccess

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.{CsvModel, DataFile, JsonModel, MultiFormatCsvModel, ParquetModel}
import com.example.fw.infra.dataaccess.impl.{CsvReaderWriter, JsonReaderWriter, StandardParquetReaderWriter}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

//TODO:型パラメータ化
trait StandardSparkDataFileReaderWriter extends DataFileReaderWriterImpl {
  override def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    inputFile match {
      case f: CsvModel[Row] => new CsvReaderWriter().readToDf(f, sparkSession)
      case f: JsonModel[Row] => new JsonReaderWriter().readToDf(f, sparkSession)
      case f: ParquetModel[Row] => new StandardParquetReaderWriter().readToDf(f, sparkSession)
      //TODO: MultiFormatCsvModel, XmlModel
    }
  }

  //TODO: 実装
  override def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = ???

  override def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode): Unit = {
    outputFile match {
      //TODO:型パラメータ化
      case f: CsvModel[T] => new CsvReaderWriter().writeFromDsDf(ds, f, saveMode)
      case f: JsonModel[T] => new JsonReaderWriter().writeFromDsDf(ds, f, saveMode)
      case f: ParquetModel[T] => new StandardParquetReaderWriter().writeFromDsDf(ds, f, saveMode)
      //TODO: MultiFormatCsvModel, XmlModel
    }
  }
}