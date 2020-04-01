package com.example.fw.domain.model

import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Dataset, Row}

case class CsvModel(path: String) extends DataFile[Row] {
  require(path != null && path.length > 0)
  override val filePath: String = path

  //TODO Dataset[T]で型パラメータ化したいがSparkSessionを渡せないと暗黙の型変換が使えないので難しい
  override def read(reader: DataFrameReader): Dataset[Row] = {
    //TODO:optionの切り替え対応(header, inferSchema, delimiter, encoding)
    reader.option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
  }

  override def write(writer: DataFrameWriter[Row]): Unit = {
    writer.csv(filePath)
  }

}
