package com.example.fw

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataFrameReaderWriter {
  def read(sparkSession: SparkSession, inputFile: String): DataFrame = {
    sparkSession.read.json(inputFile)
  }

  def write(dataFrame: DataFrame, outputFile: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
    dataFrame.write.mode(saveMode).parquet(outputFile)
  }
}
