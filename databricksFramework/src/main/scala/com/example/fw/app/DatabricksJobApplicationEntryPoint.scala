package com.example.fw.app

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.LogicCreator
import com.example.fw.domain.utils.ResourceBundleManager
import com.example.fw.infra.dataaccess.DatabricksDataFileReaderWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Databricksでjarジョブを実行する場合に使用するエントリポイント
 *
 */
object DatabricksJobApplicationEntryPoint {
  /**
   * DatabricksジョブによるSpark APを起動するためのメイン関数
   * @param args 1つ以上の引数を渡す必要がある。
   *             - 引数1として、Logicクラスの完全修飾名。
   *             - 引数2以降は、オプションで、Logicクラス実行時に渡す引数。
   */
  def main(args: Array[String]): Unit = {
    assert(args.length > 0)
    //TODO: コンストラクタ引数をとれるようにする
    val appName = args(0)

    //TODO:独自のプロパティではなくてSpark実行時のパラメータのほうがよいか？
    val clusterMode = ResourceBundleManager.get("clustermode")
    val logLevel = ResourceBundleManager.get("loglevel")

    //Sparkの実行
    //DatabricksでJARジョブプログラムを実行する場合共有SparkContextを使用するため
    //SparkSession.close（SparkContext.stop）を呼び出してはいけない
    //https://docs.microsoft.com/ja-jp/azure/databricks/jobs#%E5%85%B1%E6%9C%89-sparkcontext-%E3%82%92-%E4%BD%BF%E7%94%A8%E3%81%99%E3%82%8B--use-the-shared-sparkcontext
    val spark = SparkSession.builder()
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
