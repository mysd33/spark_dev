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
    val logicClassFQDN = args(0)
    val methodArgs = if (args.length > 1) args.tail else null
    DatabricksSparkSessionManager.run(logicClassFQDN, methodArgs)
  }

}
