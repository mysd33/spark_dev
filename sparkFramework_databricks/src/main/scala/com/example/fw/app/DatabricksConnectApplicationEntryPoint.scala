package com.example.fw.app

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.LogicCreator
import com.example.fw.domain.utils.ResourceBundleManager
import com.example.fw.domain.utils.Using.using
import com.example.fw.infra.dataaccess.DatabricksDataFileReaderWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DatabricksConnectApplicationEntryPoint {
  def main(args: Array[String]): Unit = {
    assert(args.length > 0)
    //TODO: コンストラクタ引数をとれるようにする
    val appName = args(0)

    //TODO:独自のプロパティではなくてSpark実行時のパラメータのほうがよいか？
    val clusterMode = ResourceBundleManager.get("clustermode")
    val logLevel = ResourceBundleManager.get("loglevel")

    //Sparkの実行
    val spark = SparkSession.builder()
      .master(clusterMode)
      .appName(appName)
      .getOrCreate()
    val sc = spark.sparkContext
    //TODO:ログが多いのでオフしている。log4j.propertiesで設定できるようにするなど検討
    sc.setLogLevel(logLevel)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //Logicインスタンスの実行
    val logic = LogicCreator.newInstance(appName,
      DatabrickDataFileReaderWriterFactory.createDataFileReaderWriter())
    logic.execute(spark)

  }
}
