package com.example.fw.app

import com.example.fw.domain.const.FWConst
import com.example.fw.domain.logic.LogicCreator
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * Databricksアプリケーション用SparkSession管理オブジェクト
 */
object DatabricksSparkSessionManager extends Logging {
  /**
   * 指定したLogicクラスを使用しアプリケーションを実行する
   *
   * @param logicClassFQDN Logicクラスの完全修飾名
   * @param args
   */
  def run(logicClassFQDN: String, args: Array[String]): Unit = {
    assert(logicClassFQDN != null)
    try {
      //Sparkの実行。Databricksの場合は、SparSessionをクローズしてはいけない。
      val spark = createSparkSession(logicClassFQDN)
      //Logicインスタンスの実行
      val logic = LogicCreator.newInstance(logicClassFQDN,
        DatabrickDataFileReaderWriterFactory.createDataFileReaderWriter(), args)
      logic.execute(spark)
    } catch {
      case e: Exception => logError("システムエラーが発生しました", e)
    }
  }

  /**
   * SparkSessionを作成する。
   *
   * @param appName SparkSessionに渡すアプリケーション名
   * @return SparkSession
   */
  def createSparkSession(appName: String) = {
    //TODO:独自のプロパティではなくてSpark実行時のパラメータのほうがよいか？
    val clusterMode = ResourceBundleManager.get(FWConst.CLUSTER_MODE_KEY)
    val logLevel = ResourceBundleManager.get(FWConst.LOG_LEVEL_KEY)
    //TODO: Config設定の検討
    //https://spark.apache.org/docs/latest/configuration.html
    val sparkSession = if (clusterMode != null && !clusterMode.isEmpty) {
      SparkSession.builder()
        .master(clusterMode)
        .appName(appName)
        .getOrCreate()
    } else {
      SparkSession.builder()
        .appName(appName)
        .getOrCreate()
    }
    //TODO:ログが多いのでオフしている。log4j.propertiesで設定できるようにするなど検討
    val sc = sparkSession.sparkContext
    sc.setLogLevel(logLevel)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    sparkSession
  }

}

