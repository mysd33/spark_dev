package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.DataFile
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class TextFileReaderWriter extends DataFileReaderWriterImpl {
  override def readToRDD(inputFile: DataFile[String], sparkSession: SparkSession): RDD[String] = {
    sparkSession.sparkContext.textFile(inputFile.filePath)
  }

  override def writeFromRDD[T](rdd: RDD[T], outputFile: DataFile[T]): Unit = {
    rdd.saveAsTextFile(outputFile.filePath)
  }
}
