package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.{RDDToDatasetBLogic, RDDToRDDBLogic}
import com.example.fw.domain.model.{CsvModel, DataFile, ParquetModel, TextFileModel}
import com.example.sample.model.WordCount
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

class SampleRDDtoDatasetBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends RDDToDatasetBLogic[WordCount](dataFileReaderWriter) {
  override val inputFiles: Seq[DataFile[String]] = TextFileModel[String]("README.md") :: Nil
  override val outputFile: DataFile[WordCount] = ParquetModel[WordCount]("WordCount2")

  override def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): Dataset[WordCount] = {
    import sparkSession.implicits._
    val textFile = inputs(0)
    val words = textFile.flatMap(line => line.split(" "))
    val wordCounts = words
      .map(word => (word, 1))
      .reduceByKey((a,b) => a+b)
      .map(t => WordCount(t._1, t._2))
      .toDS()
    wordCounts
  }

}
