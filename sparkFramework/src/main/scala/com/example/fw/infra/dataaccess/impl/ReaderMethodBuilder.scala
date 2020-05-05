package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.{Compressable, CsvModel, HavingDateFormat, HavingSchema, Splittable, TextFormat}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter}

object ReaderMethodBuilder {
  //csvのoptionの実装
  // http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader@csv(paths:String*):org.apache.spark.sql.DataFrame
  //jsonのoption実装
  // http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader@json(paths:String*):org.apache.spark.sql.DataFrame

  implicit class RichDataFrameReader(reader: DataFrameReader) {

    def buildSchema(file: HavingSchema): DataFrameReader = {
      file.schema match {
        case Some(schema) => reader.schema(schema)
        case _ => reader
      }
    }

    def buildCsvSchema[T](file: CsvModel[T]): DataFrameReader = {
      file.schema match {
        case Some(schema) => {
          reader.schema(schema)
            .option("header", file.hasHeader)
        }
        case _ => {
          //CSVの場合、schema設定がない場合は、ヘッダをもとにスキーマ推定
          assert(file.hasHeader, "HavingSchema.hasHeader must be true")
          reader
            .option("header", file.hasHeader)
            .option("inferSchema", "true")
        }
      }
    }

    def buildOptionSep(file: Splittable): DataFrameReader = {
      file.delimiter match {
        case Some(delimiter) => reader.option("sep", delimiter)
        case _ => reader
      }
    }

    def buildOptionDateFormat(file: HavingDateFormat): DataFrameReader = {
      val reader2 = file.dateFormat match {
        case Some(dateFormat) => reader.option("dateFormat", dateFormat)
        case _ => reader
      }
      file.timestampFormat match {
        case Some(timestampFormat) => reader2.option("timestampFormat", timestampFormat)
        case _ => reader2
      }
    }

    def buildOptionEncoding(file: TextFormat): DataFrameReader = {
      file.encoding match {
        case Some(encoding) => reader.option("encoding", encoding)
        case _ => reader
      }
    }

    def buildOptionCompression(file: Compressable): DataFrameReader = {
      file.compression match {
        case Some(compression) => reader.option("compression", compression)
        case _ => reader
      }
    }

  }

}
