package com.example.fw.infra.dataaccess

import com.example.fw.domain.model.{DataFile, DwDmModel, ParquetModel}
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
trait DatabricksDataFileReaderWriter extends StandardSparkDataFileReaderWriter {
  /**
   * @see com.example.fw.domain.dataaccess.DataFileReaderWriterImpl.readToRDD
   * @param inputFile    入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @return RDD
   */
  override def readToDfImpl(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    inputFile match {
      //DeltaLakeのみReaderWriterを差し替え
      case f: ParquetModel[Row] => new DeltaLakeReaderWriter().readToDf(f, sparkSession)
      //SynapseAnalyticsへの対応
      case f: DwDmModel[Row] => new SynapseAnalyticsReaderWriter().readToDf(f, sparkSession)
      case _ => super.readToDfImpl(inputFile, sparkSession)
    }
  }

  /**
   * @see com.example.fw.domain.dataaccess.DataFileReaderWriterImpl.readToDs[T](DataFile[T], SparkSession)
   * @param inputFile    入力ファイルのDataFile
   * @param sparkSession SparkSession
   * @return DataFrame
   */
  override def readToDsImpl[T <: Product : universe.TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    inputFile match {
      //DeltaLakeのみReaderWriterを差し替え
      case f: ParquetModel[T] => new DeltaLakeReaderWriter().readToDs(f, sparkSession)
      //SynapseAnalyticsへの対応
      case f: DwDmModel[T] => new SynapseAnalyticsReaderWriter().readToDs(f, sparkSession)
      case _ => super.readToDsImpl(inputFile, sparkSession)
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
      //DeltaLakeのみReaderWriterを差し替え
      case f: ParquetModel[T] => new DeltaLakeReaderWriter().writeFromDsDf(ds, f, saveMode)
      //SynapseAnalyticsへの対応
      case f: DwDmModel[T] => new SynapseAnalyticsReaderWriter().writeFromDsDf(ds, f)
      case _ => super.writeFromDsDfImpl(ds, outputFile, saveMode)
    }
  }
}