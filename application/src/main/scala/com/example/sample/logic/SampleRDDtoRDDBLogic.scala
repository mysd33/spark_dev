package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.RDDToRDDBLogic
import com.example.fw.domain.model.{DataFile, TextFileModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class SampleRDDtoRDDBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends RDDToRDDBLogic[(String, Int)](dataFileReaderWriter) {
  override val inputFiles: Seq[DataFile[String]] = TextFileModel[String]("README.md") :: Nil
  override val outputFile: DataFile[(String, Int)] = TextFileModel[(String, Int)]("WordCount")

  override def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): RDD[(String, Int)] = {
    val textFile = inputs(0)
    val words = textFile.flatMap(line => line.split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey((a,b) => a+b)
    wordCounts
  }

}
