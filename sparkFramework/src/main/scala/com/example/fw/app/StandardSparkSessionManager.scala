package com.example.fw.app

import com.example.fw.domain.const.FWConst
import com.example.fw.domain.logic.LogicCreator
import com.example.fw.domain.utils.ResourceBundleManager
import com.example.fw.domain.utils.Using._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * 標準的なSparkアプリケーション用SparkSession管理オブジェクト
 */
object StandardSparkSessionManager {

  /**
   * 指定したLogicクラスを使用しアプリケーションを実行する
   * @param logicClassFQDN Logicクラスの完全修飾名
   * @param args
   */
  def run(logicClassFQDN: String, args: Array[String]): Unit = {
    assert(logicClassFQDN != null)
    //Sparkの実行。Spark標準の場合は、SparSessionを最後にクローズする(using句）
    using(createSparkSession(logicClassFQDN)) {
      sparkSession => {
        //Logicインスタンスの実行
        val logic = LogicCreator.newInstance(logicClassFQDN,
          StandardSparkDataFileReaderWriterFactory.createDataFileReaderWriter(), args)
        logic.execute(sparkSession)
      }
    }
  }

  /**
   * SparkSessionを作成する。
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

