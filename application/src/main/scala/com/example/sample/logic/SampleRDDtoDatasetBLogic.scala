package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.logic.{RDDToDatasetBLogic, RDDToRDDBLogic}
import com.example.fw.domain.model.{CsvModel, DataModel, ParquetModel, TextLineModel}
import com.example.sample.common.entity.WordCount
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * AP基盤を使ったサンプル
 *
 * RDDToDatasetBLogicを継承し、テキスト行形式のファイルを読み込んで、Parquetファイルを出力するサンプル
 * @param dataModelReaderWriter Logicクラスが使用するDataModelReaderWriter
 */
class SampleRDDtoDatasetBLogic(dataModelReaderWriter: DataModelReaderWriter)
  extends RDDToDatasetBLogic[WordCount](dataModelReaderWriter) {

  //テキスト行形式のファイルを読み込む例
  override val inputModels: Seq[DataModel[String]] = TextLineModel[String]("README.md") :: Nil
  //Parquetファイルの書き込みの例
  override val outputModel: DataModel[WordCount] = ParquetModel[WordCount]("WordCount2")

  override def process(rddList: Seq[RDD[String]], sparkSession: SparkSession): Dataset[WordCount] = {
    import sparkSession.implicits._
    val textFile = rddList(0)
    val words = textFile.flatMap(line => line.split(" "))
    val wordCounts = words
      .map(word => (word, 1))
      .reduceByKey((a,b) => a+b)
      .map(t => WordCount(t._1, t._2))
      .toDS()
    wordCounts
  }

}
