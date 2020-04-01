package com.example.fw.domain.model

import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Dataset, Row}

case class MultiFormatCsvModel(path: String) extends DataFile[Row] {
  override val filePath: String = path

  //TODO:実装
  override def read(reader: DataFrameReader): Dataset[Row] = ???

  //TODO:各行のCSVパースどこでやる？
  // https://stackoverflow.com/questions/25259425/spark-reading-files-using-different-delimiter-than-new-line

  //TODO:実装
  override def write(writer: DataFrameWriter[Row]): Unit = ???

  //TODO:各行のCSV化どこでやる？

}
