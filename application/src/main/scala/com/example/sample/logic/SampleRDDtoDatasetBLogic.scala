package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.{RDDToDatasetBLogic, RDDToRDDBLogic}
import com.example.fw.domain.model.{CsvModel, DataModel, ParquetModel, TextLineModel}
import com.example.sample.common.entity.WordCount
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * AP基盤を使ったサンプル
 *
 * RDDToDatasetBLogicを継承し、テキスト行形式のファイルを読み込んで、Parquetファイルを出力するサンプル
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 */
class SampleRDDtoDatasetBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends RDDToDatasetBLogic[WordCount](dataFileReaderWriter) {

  //テキスト行形式のファイルを読み込む例
  override val inputFiles: Seq[DataModel[String]] = TextLineModel[String]("README.md") :: Nil
  //Parquetファイルの書き込みの例
  override val outputFile: DataModel[WordCount] = ParquetModel[WordCount]("WordCount2")

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
