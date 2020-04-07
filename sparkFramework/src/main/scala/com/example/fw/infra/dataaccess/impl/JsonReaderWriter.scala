package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import scala.reflect.runtime.universe.TypeTag

class JsonReaderWriter extends DataFileReaderWriterImpl {
  override def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .json(inputFile.filePath)
  }

  override def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    sparkSession.read
      .json(inputFile.filePath)
      .as[T]
  }

  override def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode): Unit = {
    ds.write
      .mode(saveMode)
      .json(outputFile.filePath)
  }
}
