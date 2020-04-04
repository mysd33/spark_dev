package com.example.fw.infra.dataaccess
import com.example.fw.domain.model.{DataFile, ParquetModel}
import com.example.fw.infra.dataaccess.impl.DeltaLakeReaderWriter
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

//TODO: Databricks依存のFWプロジェクトに切り出す
trait DatabricksDataFileReaderWriter extends StandardSparkDataFileReaderWriter {
  override def read(inputFile: DataFile[Row], sparkSession: SparkSession): Dataset[Row] = {
    inputFile match {
      //DeltaLakeのみReaderWriteを差し替え
      case f: ParquetModel => new DeltaLakeReaderWriter().read(f, sparkSession)
      case _ => super.read(inputFile, sparkSession)
    }
  }

  override def write(ds: Dataset[Row], outputFile: DataFile[Row], saveMode: SaveMode) = {
    outputFile match {
      //DeltaLakeのみReaderWriteを差し替え
      case f: ParquetModel => new DeltaLakeReaderWriter().write(ds, f, saveMode)
      case _ => super.write(ds, outputFile, saveMode)
    }
  }
}
