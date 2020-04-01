package com.example.fw.domain.model

import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Dataset, Row}

case class XmlModel(path: String) extends DataFile[Row] {
  override val filePath: String = path

  //TODO:実装
  override def read(reader: DataFrameReader): Dataset[Row] = ???

  //TODO:XMLパースどこでやる？

  //TODO:実装
  override def write(writer: DataFrameWriter[Row]): Unit = ???

  //TODO:XML形式にするのはどこでやる？
}
