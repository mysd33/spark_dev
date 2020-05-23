package com.example.fw.app

import com.example.fw.domain.const.FWConst
import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.{Logic, LogicCreator}
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.sql.SparkSession
import com.example.fw.domain.utils.Using._
import com.example.fw.infra.dataaccess.StandardSparkDataFileReaderWriter
import org.apache.log4j.{Level, Logger}
/**
 * Spark標準のAPIを用いたアプリケーションの実行用オブジェクト
 */
object StandardSparkApplicationRunner {

  /**
   * 指定したLogicクラスを使用しアプリケーションを実行する
   * @param logicClassFQDN Logicクラスの完全修飾名
   * @param args
   */
  def run(logicClassFQDN: String, args: Array[String]): Unit = {
    //TODO:独自のプロパティではなくてSpark実行時のパラメータのほうがよいか？
    val clusterMode = ResourceBundleManager.get(FWConst.CLUSTER_MODE_KEY)
    val logLevel = ResourceBundleManager.get(FWConst.LOG_LEVEL_KEY)
    //Sparkの実行。Spark標準の場合は、SparSessionを最後にクローズする
    using(SparkSession.builder()
      .master(clusterMode)
      .appName(logicClassFQDN)
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
      val logic = LogicCreator.newInstance(logicClassFQDN, createDataFileReaderWriter(), args)
      logic.execute(spark)
    }
  }

  /**
   * DataFileReaderWriterの実装インスタンスを生成
   * @return DataFileReaderWriter
   */
  private def createDataFileReaderWriter(): DataFileReaderWriter = {
    //Sparkの標準のDataFileReaderWriterでDIして作成
    new DataFileReaderWriter with StandardSparkDataFileReaderWriter
  }
}

