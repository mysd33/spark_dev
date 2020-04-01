package com.example.fw.domain.model

import org.apache.spark.sql.{Dataset, DataFrameReader, DataFrameWriter}

trait DataFile[T] {
  val filePath: String

  def read(reader: DataFrameReader): Dataset[T]

  def write(writer: DataFrameWriter[T]): Unit
}
