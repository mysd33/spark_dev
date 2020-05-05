package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.{Compressable, CsvModel, HavingDateFormat, HavingSchema, Partitionable, Splittable, TextFormat}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}

object WriterMethodBuilder {

  //csvのoptionの実装
  //http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter@csv(path:String):Unit

  //jsonのoption実装
  // http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter@json(path:String):Unit


  implicit class RichDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def buildOptionSep(file: Splittable): DataFrameWriter[T] = {
      file.delimiter match {
        case Some(delimiter) => writer.option("sep", delimiter)
        case _ => writer
      }
    }
    def buildOptionDateFormat(file: HavingDateFormat): DataFrameWriter[T] = {
      val writer2 = file.dateFormat match {
        case Some(dateFormat) => writer.option("dateFormat", dateFormat)
        case _ => writer
      }
      file.timestampFormat match {
        case Some(timestampFormat) => writer2.option("timestampFormat", timestampFormat)
        case _ => writer2
      }
    }

    def buildOptionCsvHeader(file: CsvModel[T]): DataFrameWriter[T] = {
      writer.option("header", file.hasHeader)
    }

    def buildOptionEncoding(file: TextFormat): DataFrameWriter[T] = {
      file.encoding match {
        case Some(encoding) => writer.option("encoding", encoding)
        case _ => writer
      }
    }

    def buildOptionCompression(file: Compressable): DataFrameWriter[T] = {
      file.compression match {
        case Some(compression) => writer.option("compression", compression)
        case _ => writer
      }
    }

    def buildPartitionBy(file: Partitionable): DataFrameWriter[T] = {
      file.partition match {
        case Some(partition) => writer.partitionBy(partition)
        case _ => writer
      }
    }


  }

}
