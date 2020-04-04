package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.DataFile
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

class StandardParquetReaderWriter extends DataFileReaderWriterImpl[Row] {
  //TODO:型パラーメータ化したい
  override def read(inputFile: DataFile[Row], sparkSession: SparkSession): Dataset[Row] = {
    import sparkSession.implicits._
    sparkSession.read
      .parquet(inputFile.filePath)
    //TODO: as使いたい
    //.as[T]
  }

  override def write(ds: Dataset[Row], outputFile: DataFile[Row], saveMode: SaveMode): Unit = {
    //TODO:パーティション対応
    ds.write.mode(saveMode).parquet(outputFile.filePath)
  }
}
