package com.example.fw.domain.model

import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Dataset, Row}

case class ParquetModel(path: String) extends DataFile[Row] {
  require(path != null && path.length > 0)

  override val filePath: String = path

  //TODO: DeltaLake("delta")に切り替え可能な仕組み
  val formatName: String = "parquet"

  //TODO Dataset[T]で型パラメータ化したいがSparkSessionを渡せないと暗黙の型変換が使えないので難しい
  override def read(reader: DataFrameReader): Dataset[Row] = {
    reader.format(formatName).load(filePath)
  }

  //TODO パーティション（partitionBy）の対応
  override def write(writer: DataFrameWriter[Row]): Unit = {
    writer.format(formatName).save(filePath)
  }
}
