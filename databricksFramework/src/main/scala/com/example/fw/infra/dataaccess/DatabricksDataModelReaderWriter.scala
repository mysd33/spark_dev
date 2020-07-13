package com.example.fw.infra.dataaccess

import com.example.fw.domain.model.{DataModel, DwDmModel, ParquetModel}
import com.example.fw.infra.dataaccess.impl.{DeltaLakeReaderWriter, SynapseAnalyticsReaderWriter}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe

/**
 * DataFileReaderWriterImplのDatabricks(およびDeltaLake)APIの場合の実装トレイト
 *
 * DataFileReaderWriter実装に切替える際に使用する。
 * {{{
 *   //example for Databricks(and Delta Lake) Spark Application
 *   new DataFileReaderWriter with DatabricksDataFileReaderWriter
 * }}}
 */
trait DatabricksDataModelReaderWriter extends StandardSparkDataModelReaderWriter {
  /**
   * @see com.example.fw.domain.dataaccess.DataFileReaderWriterImpl.readToRDD
   * @param input    入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @return RDD
   */
  override def readToDfImpl(input: DataModel[Row], sparkSession: SparkSession): DataFrame = {
    input match {
      //DeltaLakeのみReaderWriterを差し替え
      case f: ParquetModel[Row] => new DeltaLakeReaderWriter().readToDf(f, sparkSession)
      //SynapseAnalyticsへの対応
      case f: DwDmModel[Row] => new SynapseAnalyticsReaderWriter().readToDf(f, sparkSession)
      //TODO: ローカルでは別のReaderWriterの切り替え
      //TODO: SnowflakeとのReaderWriterの切り替え
      case _ => super.readToDfImpl(input, sparkSession)
    }
  }

  /**
   * @see com.example.fw.domain.dataaccess.DataFileReaderWriterImpl.readToDs[T](DataFile[T], SparkSession)
   * @param input    入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  override def readToDsImpl[T <: Product : universe.TypeTag](input: DataModel[T], sparkSession: SparkSession): Dataset[T] = {
    input match {
      //DeltaLakeのみReaderWriterを差し替え
      case f: ParquetModel[T] => new DeltaLakeReaderWriter().readToDs(f, sparkSession)
      //SynapseAnalyticsへの対応
      case f: DwDmModel[T] => new SynapseAnalyticsReaderWriter().readToDs(f, sparkSession)
      //TODO: ローカルでは別のReaderWriterの切り替え
      //TODO: SnowflakeとのReaderWriterの切り替え
      case _ => super.readToDsImpl(input, sparkSession)
    }
  }

  /**
   * @see [[com.example.fw.domain.dataaccess.DataModelReaderWriterImpl.writeFromDsDfImpl]]
   * @param ds         出力対象のDataset/DataFrame
   * @param output 出力先ファイルのDataFile
   * @param saveMode   出力時のSaveMode
   * @tparam T DataFileの型パラメータ
   */
  override def writeFromDsDfImpl[T](ds: Dataset[T], output: DataModel[T], saveMode: SaveMode): Unit = {
    output match {
      //DeltaLakeのみReaderWriterを差し替え
      case f: ParquetModel[T] => new DeltaLakeReaderWriter().writeFromDsDf(ds, f, saveMode)
      //SynapseAnalyticsへの対応
      case f: DwDmModel[T] => new SynapseAnalyticsReaderWriter().writeFromDsDf(ds, f, saveMode)
      //TODO: ローカルでは別のReaderWriterの切り替え
      //TODO: SnowflakeとのReaderWriterの切り替え
      case _ => super.writeFromDsDfImpl(ds, output, saveMode)
    }
  }
}