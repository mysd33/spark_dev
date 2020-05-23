package com.example.fw.app

/**
 * 標準的なSparkApのエントリポイントオブジェクト
 */
object ApplicationEntryPoint {
  /**
   * Spark APを起動するためのメイン関数
   *
   * @param args 1つ以上の引数を渡す必要がある。
   *             - 引数1として、Logicクラスの完全修飾名。
   *             - 引数2以降は、オプションで、Logicクラス実行時に渡す引数。
   */
  def main(args: Array[String]): Unit = {
    assert(args.length > 0)
    val logicClassFQDN = args(0)
    val methodArgs = if (args.length > 1) args.tail else null
    StandardSparkApplicationRunner.run(logicClassFQDN, methodArgs)
  }

}
