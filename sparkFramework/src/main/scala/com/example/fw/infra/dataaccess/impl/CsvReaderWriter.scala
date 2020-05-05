package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.CsvModel
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import scala.reflect.runtime.universe.TypeTag

import com.example.fw.infra.dataaccess.impl.ReaderMethodBuilder._
import com.example.fw.infra.dataaccess.impl.WriterMethodBuilder._

class CsvReaderWriter {

  def readToDf(inputFile: CsvModel[Row], sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      //暗黙の型変換でメソッド拡張
      .buildOptionSep(inputFile)
      .buildOptionDateFormat(inputFile)
      .buildCsvSchema(inputFile)
      .buildOptionEncoding(inputFile)
      .buildOptionCompression(inputFile)
      .csv(inputFile.absolutePath)
  }

  def readToDs[T <: Product : TypeTag](inputFile: CsvModel[T], sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._
    sparkSession.read
      //暗黙の型変換でメソッド拡張
      .buildOptionSep(inputFile)
      .buildOptionDateFormat(inputFile)
      .buildCsvSchema(inputFile)
      .buildOptionEncoding(inputFile)
      .buildOptionCompression(inputFile)
      .csv(inputFile.absolutePath).as[T]
  }

  def writeFromDsDf[T](ds: Dataset[T], outputFile: CsvModel[T], saveMode: SaveMode): Unit = {
    ds.write.mode(saveMode)
      //暗黙の型変換でメソッド拡張
      .buildOptionSep(outputFile)
      .buildOptionCsvHeader(outputFile)
      .buildOptionDateFormat(outputFile)
      .buildOptionCompression(outputFile)
      .buildOptionEncoding(outputFile)
      .buildPartitionBy(outputFile)
      .csv(outputFile.absolutePath)
  }

}
