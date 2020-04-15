package com.example.fw.domain.utils

import org.apache.spark.internal.Logging

object Using extends Logging {
  //scala2.13.0からは標準でUsingクラスをサポートしているが、Spark2.4.5は、scala2.12（Databricksは2.11）対応のため
  def using[A <: {def close()}, B](resource: A)(func: A => B): Option[B] =
    try {
      Some(func(resource)) //成功したら、Someに包んで返す
    } catch {
      case e: Exception => logError("システムエラーが発生しました", e)
        None //失敗したら、ログ吐いて、None返す
    } finally {
      if (resource != null) resource.close()
    }
}
