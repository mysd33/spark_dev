package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.{DataFile, JsonModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.reflect.runtime.universe.TypeTag
import com.example.fw.infra.dataaccess.impl.ReaderMethodBuilder._
import com.example.fw.infra.dataaccess.impl.WriterMethodBuilder._

class JsonReaderWriter {
  def readToDf(inputFile: JsonModel[Row], sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .buildSchema(inputFile)
      .buildOptionDateFormat(inputFile)
      .buildOptionEncoding(inputFile)
      .buildOptionCompression(inputFile)
      .json(inputFile.absolutePath)
  }

  def readToDs[T <: Product : TypeTag](inputFile: JsonModel[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    sparkSession.read
      //暗黙の型変換でメソッド拡張
      .buildSchema(inputFile)
      .buildOptionDateFormat(inputFile)
      .buildOptionEncoding(inputFile)
      .buildOptionCompression(inputFile)
      .json(inputFile.absolutePath).as[T]
  }

  def writeFromDsDf[T](ds: Dataset[T], outputFile: JsonModel[T], saveMode: SaveMode): Unit = {
    ds.write.mode(saveMode)
      //暗黙の型変換でメソッド拡張
      .buildOptionDateFormat(outputFile)
      .buildOptionEncoding(outputFile)
      .buildOptionCompression(outputFile)
      .buildPartitionBy(outputFile)
      .json(outputFile.absolutePath)
  }
}
