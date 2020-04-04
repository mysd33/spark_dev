package com.example.fw.infra.dataaccess

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.{CsvModel, DataFile, JsonModel, ParquetModel}
import com.example.fw.infra.dataaccess.impl.{CsvReaderWriter, JsonReaderWriter, StandardParquetReaderWriter}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Dataset, Encoder, Row, SaveMode, SparkSession}

//TODO:型パラメータ化
trait StandardSparkDataFileReaderWriter extends DataFileReaderWriterImpl[Row] {
  override def read(inputFile: DataFile[Row], sparkSession: SparkSession): Dataset[Row] = {
    //TODO: フォーマットの追加への柔軟性を考えるとmatch-caseを使うよりもMapを使うべきでは？
    inputFile match {
      case f: CsvModel => new CsvReaderWriter().read(f, sparkSession)
      case f: JsonModel => new JsonReaderWriter().read(f, sparkSession)
      case f: ParquetModel => new StandardParquetReaderWriter().read(f, sparkSession)
    }
  }

  override def write(ds: Dataset[Row], outputFile: DataFile[Row], saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    //TODO: フォーマットの追加への柔軟性を考えるとmatch-caseを使うよりもMapを使うべきでは？
    outputFile match {
      //TODO:型パラメータ化
      case f: CsvModel => new CsvReaderWriter().write(ds, f, saveMode)
      case f: JsonModel => new JsonReaderWriter().write(ds, f, saveMode)
      case f: ParquetModel => new StandardParquetReaderWriter().write(ds, f, saveMode)
    }
  }
}