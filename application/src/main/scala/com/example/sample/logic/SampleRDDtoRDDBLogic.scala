package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.RDDToRDDBLogic
import com.example.fw.domain.model.{DataModel, TextLineModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * AP基盤を使ったサンプル
 *
 * RDDToRDDBLogicを継承し、テキスト行形式のファイルを読み込んで、テキスト行形式のファイルを出力するサンプル
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 */
class SampleRDDtoRDDBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends RDDToRDDBLogic[(String, Int)](dataFileReaderWriter) {
  override val inputFiles: Seq[DataModel[String]] = TextLineModel[String]("README.md") :: Nil
  override val outputFile: DataModel[(String, Int)] = TextLineModel[(String, Int)]("WordCount")

  override def process(inputs: Seq[RDD[String]], sparkSession: SparkSession): RDD[(String, Int)] = {
    val textFile = inputs(0)
    val words = textFile.flatMap(line => line.split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey((a,b) => a+b)
    wordCounts
  }

}
