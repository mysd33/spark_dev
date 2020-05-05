package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.model.TextLineModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class TextLineFileReaderWriter {
  def readToRDD(inputFile: TextLineModel[String], sparkSession: SparkSession): RDD[String] = {
    sparkSession.sparkContext.textFile(inputFile.absolutePath)
  }

  def writeFromRDD[T](rdd: RDD[T], outputFile: TextLineModel[T]): Unit = {
    rdd.saveAsTextFile(outputFile.absolutePath)
  }
}
