package com.example.fw.domain.model

import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Dataset, Row}

case class JsonModel(path: String) extends DataFile[Row] {
  require(path != null && path.length > 0)

  override val filePath: String = path

  //TODO Dataset[T]で型パラメータ化したいがSparkSessionを渡せないと暗黙の型変換が使えないので難しい
  override def read(reader: DataFrameReader): Dataset[Row] = {
    reader.json(filePath)
  }

  override def write(writer: DataFrameWriter[Row]): Unit = {
    writer.json(filePath)
  }

}
