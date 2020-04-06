package com.example.fw.app

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.{Logic, LogicCreator}
import com.example.fw.domain.utils.Using.using
import com.example.fw.infra.dataaccess.DatabricksDataFileReaderWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import scala.reflect.runtime.{universe => ru}

object DatabricksApplicationEntryPoint {
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
      //TODO:ログが多いのでオフしている。log4j.propertiesで設定できるようにするなど検討
      sc.setLogLevel(logLevel)
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      //Logicインスタンスの実行
      val logic = LogicCreator.newInstance(appName, createDataFileReaderWriter())
      logic.execute(spark)
    }
  }

  private def createDataFileReaderWriter(): DataFileReaderWriter[Row] = {
    //Databricks用のReaderWriterに差し替え
    new DataFileReaderWriter[Row] with DatabricksDataFileReaderWriter
  }

}
