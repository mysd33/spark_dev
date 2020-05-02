package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import scala.reflect.runtime.universe.TypeTag

class JsonReaderWriter extends DataFileReaderWriterImpl {
  override def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    val reader = sparkSession.read
    val reader2 = inputFile.encoding match {
      case Some(encoding) => reader.option("encoding", encoding)
      case _ => reader
    }
    reader2.json(inputFile.filePath)
  }

  override def readToDs[T <: Product : TypeTag](inputFile: DataFile[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    val reader = sparkSession.read
    val reader2 = inputFile.encoding match {
      case Some(encoding) => reader.option("encoding", encoding)
      case _ => reader
    }
    reader2.json(inputFile.filePath).as[T]
  }

  override def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode): Unit = {
    val writer = ds.write.mode(saveMode)
    val writer2 = outputFile.partition match {
      case Some(partition) => writer.partitionBy(partition)
      case _ => writer
    }
    val writer3 = outputFile.encoding match {
      case Some(encoding) => writer2.option("encoding", encoding)
      case _ => writer2
    }
    writer3.json(outputFile.filePath)
  }
}
