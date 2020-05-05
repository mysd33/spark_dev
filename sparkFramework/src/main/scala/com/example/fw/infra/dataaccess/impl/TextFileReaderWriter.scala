package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.TextFileModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class TextFileReaderWriter {
  def readToRDD(inputFile: TextFileModel[String], sparkSession: SparkSession): RDD[String] = {
    sparkSession.sparkContext.textFile(inputFile.filePath)
  }

  def writeFromRDD[T](rdd: RDD[T], outputFile: TextFileModel[T]): Unit = {
    rdd.saveAsTextFile(outputFile.filePath)
  }
}
