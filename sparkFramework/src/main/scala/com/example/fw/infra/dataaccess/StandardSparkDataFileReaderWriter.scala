package com.example.fw.infra.dataaccess

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.{CsvModel, DataFile, JsonModel, MultiFormatCsvModel, ParquetModel, TextLineModel, XmlModel}
import com.example.fw.infra.dataaccess.impl.{CsvReaderWriter, JsonReaderWriter, MultiFormatCsvReaderWriter, StandardParquetReaderWriter, TextLineFileReaderWriter, XmlReaderWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * DataFileReaderWriterImplのSpark標準APIの場合の実装トレイト
 *
 * DataFileReaderWriter実装に切替える際に使用する。
 * {{{
 *   //example for Standard Spark Application
 *   new DataFileReaderWriter with StandardSparkDataFileReaderWriter
 * }}}
 */
trait StandardSparkDataFileReaderWriter extends DataFileReaderWriterImpl {
  /**
   * @see [[com.example.fw.domain.dataaccess.DataFileReaderWriterImpl.readToRDDImpl]]
   * @param inputFile    入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @return RDD
   */
  override def readToRDDImpl(inputFile: DataFile[String], sparkSession: SparkSession): RDD[String] = {
    inputFile match {
      case f: TextLineModel[String] => new TextLineFileReaderWriter().readToRDD(f, sparkSession)
      case f: MultiFormatCsvModel[String] => new MultiFormatCsvReaderWriter().readToRDD(f, sparkSession)
      case _ => ???
    }
  }

  /**
   * @see [[com.example.fw.domain.dataaccess.DataFileReaderWriterImpl.readToDfImpl]]
   * @param inputFile    入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  override def readToDfImpl(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    inputFile match {
      case f: CsvModel[Row] => new CsvReaderWriter().readToDf(f, sparkSession)
      case f: JsonModel[Row] => new JsonReaderWriter().readToDf(f, sparkSession)
      case f: ParquetModel[Row] => new StandardParquetReaderWriter().readToDf(f, sparkSession)
      case f: TextLineModel[Row] => new TextLineFileReaderWriter().readToDf(f, sparkSession)
      //TODO: spark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しないので本番開発では使用しない
      case f: XmlModel[Row] => new XmlReaderWriter().readToDf(f, sparkSession)
      case _ => ???
    }
  }

  /**
   * @see com.example.fw.domain.dataaccess.DataFileReaderWriterImpl.readToDs[T](DataFile[T], SparkSession)
   * @param inputFile    入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @tparam T DataFileおよびDatasetの型パラメータ
   * @return Dataset
   */
  override def readToDsImpl[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    inputFile match {
      case f: CsvModel[T] => new CsvReaderWriter().readToDs(f, sparkSession)
      case f: JsonModel[T] => new JsonReaderWriter().readToDs(f, sparkSession)
      case f: ParquetModel[T] => new StandardParquetReaderWriter().readToDs(f, sparkSession)
      case _ => ???
    }
  }

  /**
   *　@see com.example.fw.domain.dataaccess.DataFileReaderWriterImpl.readToDs(DataFile[String], SparkSession)
   * @param inputFile TextLineModel
   * @param sparkSession TextLineModel
   * @return Dataset
   */
  override def readToDsImpl(inputFile: DataFile[String], sparkSession: SparkSession): Dataset[String] = {
    inputFile match {
      case f: TextLineModel[String] => new TextLineFileReaderWriter().readToDs(f, sparkSession)
      case _ => ???
    }
  }

  /**
   * @see [[com.example.fw.domain.dataaccess.DataFileReaderWriterImpl.writeFromRDDImpl]]
   * @param rdd        出力対象のRDD
   * @param outputFile 出力先ファイルのDataFile
   * @tparam T RDDおよびDataFileの型パラメータ
   */
  override def writeFromRDDImpl[T](rdd: RDD[T], outputFile: DataFile[T]): Unit = {
    outputFile match {
      case f: TextLineModel[T] => new TextLineFileReaderWriter().writeFromRDD(rdd, f)
      case _ => ???
    }
  }

  /**
   * @see [[com.example.fw.domain.dataaccess.DataFileReaderWriterImpl.writeFromDsDfImpl]]
   * @param ds         出力対象のDataset/DataFrame
   * @param outputFile 出力先ファイルのDataFile
   * @param saveMode   出力時のSaveMode
   * @tparam T DataFileの型パラメータ
   */
  override def writeFromDsDfImpl[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode): Unit = {
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