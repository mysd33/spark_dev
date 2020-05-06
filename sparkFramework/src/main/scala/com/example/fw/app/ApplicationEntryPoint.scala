package com.example.fw.app

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.LogicCreator
import com.example.fw.domain.utils.Using.using
import com.example.fw.infra.dataaccess.StandardSparkDataFileReaderWriter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.util.ResourceBundle

import com.example.fw.domain.utils.ResourceBundleManager

/**
 * 標準的なSparkApのエントリポイントオブジェクト
 */
object ApplicationEntryPoint {
  /**
   * Spark APを起動するためのメイン関数
   * @param args 1つ以上の引数を渡す必要がある。
   *             - 引数1として、Logicクラスの完全修飾名。
   *             - 引数2以降は、オプションで、Logicクラス実行時に渡す引数。
   */
  def main(args: Array[String]): Unit = {
    assert(args.length > 0)
    val appName = args(0)
    val methodArgs = if (args.length > 1) args.tail else null
    //TODO:独自のプロパティではなくてSpark実行時のパラメータのほうがよいか？
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
      val logic = LogicCreator.newInstance(appName, createDataFileReaderWriter(), methodArgs)
      logic.execute(spark)
    }
  }

  private def createDataFileReaderWriter(): DataFileReaderWriter = {
    //Sparkの標準のDataFileReaderWriterで作成
    new DataFileReaderWriter with StandardSparkDataFileReaderWriter
  }
}
