package com.example.fw.domain.utils

import org.apache.spark.internal.Logging

/**
 * .NETのusing句相当(javaのtry-with-resource句相当)の機能を提供する
 * ユーティリティオブジェクト
 *
 * try-finally構文をラップし、例外の発生有無に係わらず
 * 利用者が忘れずにリソースをclose（解放）するようにする糖衣構文を実装する。
 * {{{
 *   //using句で生成したインスタンス
 *   using(createSparkSession(logicClassFQDN)) {
 *       sparkSession => {
 *         val logic = LogicCreator.newInstance(logicClassFQDN,
 *           StandardSparkDataFileReaderWriterFactory.createDataFileReaderWriter(), args)
 *         logic.execute(sparkSession)
 *       }
 *     }
 * }}}
 *
 * scala2.13.0からは標準でUsingクラスをサポートしているが、
 * Spark2.4.5は、scala2.12（Databricksは2.11）サポートのため
 * scala2.13.0は使用できないので、個別実装している
 */
object Using extends Logging {
  /**
   * using句を提供する
   *
   * @param resource 処理終了後、closeしたいリソース。closeメソッドを定義している必要がある。
   * @param func     リソースを入力とする処理を実装する関数
   * @tparam A resoucrceの型
   * @tparam B funcの戻り値の型
   * @return funcの戻り値をOption化したもの
   *         - 成功したらSomeに包んで返却
   *         - 例外は発生した場合は、エラーログを出力しNoneを返却
   */
  def using[A <: {def close()}, B](resource: A)(func: A => B): Option[B] =
    try {
      Some(func(resource))
    } catch {
      case e: Exception => {
        logError("システムエラーが発生しました", e)
        throw e
      }
    } finally {
      if (resource != null) resource.close()
    }
}
