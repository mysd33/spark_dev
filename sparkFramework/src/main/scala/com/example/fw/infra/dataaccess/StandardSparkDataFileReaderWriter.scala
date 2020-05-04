package com.example.fw.infra.dataaccess

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.{CsvModel, DataFile, JsonModel, MultiFormatCsvModel, ParquetModel, TextFileModel, XmlModel}
import com.example.fw.infra.dataaccess.impl.{CsvReaderWriter, JsonReaderWriter, MultiFormatCsvReaderWriter, StandardParquetReaderWriter, TextFileReaderWriter, XmlReaderWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

trait StandardSparkDataFileReaderWriter extends DataFileReaderWriterImpl {
  override def readToRDD(inputFile: DataFile[String], sparkSession: SparkSession): RDD[String] = {
    inputFile match {
      case f: TextFileModel[String] => new TextFileReaderWriter().readToRDD(f, sparkSession)
      case f: MultiFormatCsvModel[String] => new MultiFormatCsvReaderWriter().readToRDD(f, sparkSession)
      case _ => ???
    }
  }

  override def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    inputFile match {
      case f: CsvModel[Row] => new CsvReaderWriter().readToDf(f, sparkSession)
      case f: JsonModel[Row] => new JsonReaderWriter().readToDf(f, sparkSession)
      case f: ParquetModel[Row] => new StandardParquetReaderWriter().readToDf(f, sparkSession)
      //TODO: spark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しないので本番開発では使用しない
      case f: XmlModel[Row] => new XmlReaderWriter().readToDf(f, sparkSession)
      case _ => ???
    }
  }

  override def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    inputFile match {
      case f: CsvModel[T] => new CsvReaderWriter().readToDs(f, sparkSession)
      case f: JsonModel[T] => new JsonReaderWriter().readToDs(f, sparkSession)
      case f: ParquetModel[T] => new StandardParquetReaderWriter().readToDs(f, sparkSession)
      case _ => ???
    }
  }

  override def writeFromRDD[T](rdd: RDD[T], outputFile: DataFile[T]): Unit = {
    outputFile match {
      case f: TextFileModel[T] => new TextFileReaderWriter().writeFromRDD(rdd, f)
      case _ => ???
    }
  }

  override def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode): Unit = {
    outputFile match {
      case f: CsvModel[T] => new CsvReaderWriter().writeFromDsDf(ds, f, saveMode)
      case f: JsonModel[T] => new JsonReaderWriter().writeFromDsDf(ds, f, saveMode)
      case f: ParquetModel[T] => new StandardParquetReaderWriter().writeFromDsDf(ds, f, saveMode)
      //TODO: spark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しないので本番開発では使用しない
      case f: XmlModel[T] => new XmlReaderWriter().writeFromDsDf(ds, f, saveMode)
      case _ => ???
    }
  }
}