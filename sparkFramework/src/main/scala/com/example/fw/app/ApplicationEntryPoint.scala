package com.example.fw.app

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.{Logic, LogicCreator}
import com.example.fw.domain.utils.Using.using
import com.example.fw.infra.dataaccess.StandardSparkDataFileReaderWriter
import org.apache.spark.sql.{Row, SparkSession}

import scala.reflect.runtime.{universe => ru}

object ApplicationEntryPoint {
  def main(args: Array[String]): Unit = {
    assert(args.length > 0)
    //TODO: コンストラクタ引数をとれるようにする
    val appName = args(0)
    //TODO: localモードかどうかの切替え
    val master = "local[*]"
    //TODO: プロパティで切替え
    val logLevel = "INFO"
    //Sparkの実行
    using(SparkSession.builder()
      .master(master)
      .appName(appName)
      .getOrCreate()
    ) { spark =>
      val sc = spark.sparkContext
      sc.setLogLevel(logLevel)
      //Logicインスタンスの実行
      val logic = LogicCreator.newInstance(appName, createDataFileReaderWriter())
      logic.execute(spark)
    }
  }

  private def createDataFileReaderWriter(): DataFileReaderWriter[Row] = {
    //Sparkの標準のDataFileReaderWriterで作成
    new DataFileReaderWriter[Row] with StandardSparkDataFileReaderWriter
  }
}
