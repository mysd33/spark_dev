package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import scala.reflect.runtime.universe.TypeTag

class StandardParquetReaderWriter {

  def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .parquet(inputFile.filePath)
  }

  def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    sparkSession.read
      .parquet(inputFile.filePath)
    .as[T]
  }

  def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode): Unit = {
    val writer = ds.write.mode(saveMode)
    val writer2 = outputFile.partition match {
      case Some(partition) => writer.partitionBy(partition)
      case _ => writer
    }
    writer2.parquet(outputFile.filePath)
  }
}
