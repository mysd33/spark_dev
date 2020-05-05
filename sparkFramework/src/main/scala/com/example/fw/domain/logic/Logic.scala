package com.example.fw.domain.logic

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * Logicクラスの最上位の基底トレイト。
 *
 * ジョブ設計書に記載したビジネスロジックを実装する。
 *
 * org.apache.spark.internal.Loggingによるロギング機能を持っている。
 */
trait Logic extends Logging with Serializable {
  /**
   * ビジネスロジックを実行する
   * @param sparkSession SparkSession
   */
  def execute(sparkSession: SparkSession): Unit
}
