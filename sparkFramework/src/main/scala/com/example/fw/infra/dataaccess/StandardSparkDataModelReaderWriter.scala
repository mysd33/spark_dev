package com.example.fw.infra.dataaccess

import com.example.fw.domain.dataaccess.DataModelReaderWriterImpl
import com.example.fw.domain.model.{CsvModel, DataModel, JsonModel, MultiFormatCsvModel, ParquetModel, TextLineModel, XmlModel}
import com.example.fw.infra.dataaccess.impl.{CsvReaderWriter, JsonReaderWriter, MultiFormatCsvReaderWriter, StandardParquetReaderWriter, TextLineFileReaderWriter, XmlReaderWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * DataModelReaderWriterImplのSpark標準APIの場合の実装トレイト
 *
 * DataModelReaderWriter実装に切替える際に使用する。
 * {{{
 *   //example for Standard Spark Application
 *   new DataModelReaderWriter with StandardSparkDataModelReaderWriter
 * }}}
 */
trait StandardSparkDataModelReaderWriter extends DataModelReaderWriterImpl {
  /**
   * @see [[com.example.fw.domain.dataaccess.DataModelReaderWriterImpl.readToRDDImpl]]
   * @param input    入力ファイルのDataModel
   * @param sparkSession SparkSession
   * @return RDD
   */
  override def readToRDDImpl(input: DataModel[String], sparkSession: SparkSession): RDD[String] = {
    input match {
      case f: TextLineModel[String] => new TextLineFileReaderWriter().readToRDD(f, sparkSession)
      case f: MultiFormatCsvModel[String] => new MultiFormatCsvReaderWriter().readToRDD(f, sparkSession)
      case _ => ???
    }
  }

  /**
   * @see [[com.example.fw.domain.dataaccess.DataModelReaderWriterImpl.readToDfImpl]]
   * @param input    入力ファイルのDataModel
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  override def readToDfImpl(input: DataModel[Row], sparkSession: SparkSession): DataFrame = {
    input match {
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
   * @see com.example.fw.domain.dataaccess.DataModelReaderWriterImpl.readToDs[T](DataModel[T], SparkSession)
   * @param input    入力ファイルのDataModel
   * @param sparkSession SparkSession
   * @tparam T DataModelおよびDatasetの型パラメータ
   * @return Dataset
   */
  override def readToDsImpl[T <: Product : TypeTag](input: DataModel[T], sparkSession: SparkSession): Dataset[T] = {
    input match {
      case f: CsvModel[T] => new CsvReaderWriter().readToDs(f, sparkSession)
      case f: JsonModel[T] => new JsonReaderWriter().readToDs(f, sparkSession)
      case f: ParquetModel[T] => new StandardParquetReaderWriter().readToDs(f, sparkSession)
      case _ => ???
    }
  }

  /**
   *　@see com.example.fw.domain.dataaccess.DataModelReaderWriterImpl.readToDs(DataModel[String], SparkSession)
   * @param input TextLineModel
   * @param sparkSession TextLineModel
   * @return Dataset
   */
  override def readToDsImpl(input: DataModel[String], sparkSession: SparkSession): Dataset[String] = {
    input match {
      case f: TextLineModel[String] => new TextLineFileReaderWriter().readToDs(f, sparkSession)
      case _ => ???
    }
  }

  /**
   * @see [[com.example.fw.domain.dataaccess.DataModelReaderWriterImpl.writeFromRDDImpl]]
   * @param rdd        出力対象のRDD
   * @param output 出力先ファイルのDataModel
   * @tparam T RDDおよびDataModelの型パラメータ
   */
  override def writeFromRDDImpl[T](rdd: RDD[T], output: DataModel[T]): Unit = {
    output match {
      case f: TextLineModel[T] => new TextLineFileReaderWriter().writeFromRDD(rdd, f)
      case _ => ???
    }
  }

  /**
   * @see [[com.example.fw.domain.dataaccess.DataModelReaderWriterImpl.writeFromDsDfImpl]]
   * @param ds         出力対象のDataset/DataFrame
   * @param output 出力先ファイルのDataModel
   * @param saveMode   出力時のSaveMode
   * @tparam T DataModelの型パラメータ
   */
  override def writeFromDsDfImpl[T](ds: Dataset[T], output: DataModel[T], saveMode: SaveMode): Unit = {
    output match {
      case f: CsvModel[T] => new CsvReaderWriter().writeFromDsDf(ds, f, saveMode)
      case f: JsonModel[T] => new JsonReaderWriter().writeFromDsDf(ds, f, saveMode)
      case f: ParquetModel[T] => new StandardParquetReaderWriter().writeFromDsDf(ds, f, saveMode)
      //TODO: spark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しないので本番開発では使用しない
      case f: XmlModel[T] => new XmlReaderWriter().writeFromDsDf(ds, f, saveMode)
      case _ => ???
    }
  }
}