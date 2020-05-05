package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.ParquetModel
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag

import com.example.fw.infra.dataaccess.impl.WriterMethodBuilder._

class StandardParquetReaderWriter {

  def readToDf(inputFile: ParquetModel[Row], sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .parquet(inputFile.absolutePath)
  }

  def readToDs[T <: Product : TypeTag](inputFile: ParquetModel[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    sparkSession.read
      .parquet(inputFile.absolutePath)
      .as[T]
  }

  def writeFromDsDf[T](ds: Dataset[T], outputFile: ParquetModel[T], saveMode: SaveMode): Unit = {
    ds.write.mode(saveMode)
      //暗黙の型変換でメソッド拡張
      .buildOptionCompression(outputFile)
      .buildPartitionBy(outputFile)
      .parquet(outputFile.absolutePath)
  }
}
