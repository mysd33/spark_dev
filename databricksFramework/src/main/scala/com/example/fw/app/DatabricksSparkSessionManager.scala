package com.example.fw.app

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.example.fw.domain.const.FWConst
import com.example.fw.domain.logging.Log4jConfiguration
import com.example.fw.domain.logic.LogicCreator
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * Databricksアプリケーション用SparkSession管理オブジェクト
 */
object DatabricksSparkSessionManager extends Logging {
  private val SQLDW_BLOB_ACCOUNTKEY_NAME = "sqldw.blob.accountkey.name"
  private val SQLDW_BLOB_ACCOUNTKEY_SCOPE = "sqldw.blob.accountkey.scope"
  private val SQLDW_BLOB_ACCOUNTKEY_KEY = "sqldw.blob.accountkey.key"

  /**
   * 指定したLogicクラスを使用しアプリケーションを実行する
   *
   * @param logicClassFQDN Logicクラスの完全修飾名
   * @param args Logicクラスの起動引数
   */
  def run(logicClassFQDN: String, args: Array[String]): Unit = {
    assert(logicClassFQDN != null)
    try {
      //Sparkの実行。Databricksの場合は、SparSessionをクローズしてはいけない。
      val spark = createSparkSession(logicClassFQDN)
      //Logicインスタンスの実行
      val logic = LogicCreator.newInstance(logicClassFQDN,
        DatabricksDataModelReaderWriterFactory.createDataModelReaderWriter(), args)
      logic.execute(spark)
    } catch {
      case e: Exception => {
        logError("システムエラーが発生しました", e)
        throw e
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
    configureSynapseAnalytics(sparkSession)
    sparkSession
  }

  /**
   * SynapseAnalyticsの接続情報を設定する
   * @param sparkSession SparkSession
   */
  def configureSynapseAnalytics(sparkSession: SparkSession) = {
    //Synapse Analyticsによる仲介用のBLobストレージアカウントキーの設定
    val accountKeyName = ResourceBundleManager.get(SQLDW_BLOB_ACCOUNTKEY_NAME)
    val accountKeyScope = ResourceBundleManager.get(SQLDW_BLOB_ACCOUNTKEY_SCOPE)
    val accountKeyKey = ResourceBundleManager.get(SQLDW_BLOB_ACCOUNTKEY_KEY)
    if (accountKeyKey != null && accountKeyScope != null && accountKeyKey != null) {
      //DBUtilsはローカル環境では動かないので注意
      sparkSession.conf.set(accountKeyName, dbutils.secrets.get(accountKeyScope, accountKeyKey))
    }
  }
}

