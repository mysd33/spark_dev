package com.example.fw.app

import com.example.fw.domain.logic.LogicCreator
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * 端末からDatabricks接続する場合のエントリポイントの基底クラス
 *
 * 当該クラスを継承し、プロジェクトのjarをaddjarメソッドで追加した実装クラスを用意する。
 * {{{
 * object EntryPoint extends DatabricksConnectApplicationEntryPoint {
 *   def main(args: Array[String]): Unit = {
 *     run(args)
 *   }
 *
 *   //override addJar method*
 *   override protected def addJar(sc: SparkContext): Unit = {
 *     // add project jar
 *     sc.addJar("target/scala-2.11/xxxxx-assembly-0.1.jar")
 *   }
 * }
 * }}}
 */
abstract class DatabricksConnectApplicationEntryPoint {

  /**
   * SparkAPを起動する
   *
   * @param args 1つ以上の引数を渡す必要がある。
   *             - 引数1として、Logicクラスの完全修飾名。
   *             - 引数2以降は、オプションで、Logicクラス実行時に渡す引数。
   */
  final def run(args: Array[String]): Unit = {
    assert(args.length > 0)
    val appName = args(0)
    val methodArgs = if (args.length > 1) args.tail else null
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
    //Jarの追加
    addJar(sc)

    //Logicインスタンスの実行
    val logic = LogicCreator.newInstance(appName,
      DatabrickDataFileReaderWriterFactory.createDataFileReaderWriter(), methodArgs)
    logic.execute(spark)
  }

  /**
   * Databricks接続での実行時にクラスタに送信するプロジェクトのjarを追加する実装を行う
   * {{{
   *
   * }}}
   *
   * @param sc
   */
  def addJar(sc: SparkContext)

}
