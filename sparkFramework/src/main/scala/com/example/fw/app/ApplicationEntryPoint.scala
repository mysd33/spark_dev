package com.example.fw.app

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.LogicCreator
import com.example.fw.domain.utils.Using.using
import com.example.fw.infra.dataaccess.StandardSparkDataFileReaderWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.util.ResourceBundle

import com.example.fw.domain.utils.ResourceBundleManager

object ApplicationEntryPoint {
  def main(args: Array[String]): Unit = {
    assert(args.length > 0)
    //TODO: コンストラクタ引数をとれるようにする
    val appName = args(0)
    val clusterMode = ResourceBundleManager.get("clustermode")
    val logLevel = ResourceBundleManager.get("loglevel")

    //Sparkの実行
    using(SparkSession.builder()
      .master(clusterMode)
      .appName(appName)
      //TODO: Config設定の検討
      //https://spark.apache.org/docs/latest/configuration.html
      //.config("key", "value")
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

  private def createDataFileReaderWriter(): DataFileReaderWriter = {
    //Sparkの標準のDataFileReaderWriterで作成
    new DataFileReaderWriter with StandardSparkDataFileReaderWriter
  }
}
