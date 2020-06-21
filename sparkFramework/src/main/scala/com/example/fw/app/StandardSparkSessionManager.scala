package com.example.fw.app

import com.example.fw.domain.const.FWConst
import com.example.fw.domain.logging.Log4jConfiguration
import com.example.fw.domain.logic.LogicCreator
import com.example.fw.domain.utils.ResourceBundleManager
import com.example.fw.domain.utils.Using._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * 標準的なSparkアプリケーション用SparkSession管理オブジェクト
 */
object StandardSparkSessionManager extends Logging {

  /**
   * 指定したLogicクラスを使用しアプリケーションを実行する
   *
   * @param logicClassFQDN Logicクラスの完全修飾名
   * @param args Logicクラスの起動引数
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
   *
   * @param appName SparkSessionに渡すアプリケーション名
   * @return SparkSession SparkSession
   */
  def createSparkSession(appName: String): SparkSession = {
    val clusterMode = ResourceBundleManager.get(FWConst.CLUSTER_MODE_KEY)

    //log4j.propertiesの適用
    Log4jConfiguration.configure()

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
    sparkSession
  }

}

