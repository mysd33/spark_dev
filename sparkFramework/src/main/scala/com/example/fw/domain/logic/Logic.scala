package com.example.fw.domain.logic

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

//TODO: org.apache.spark.internal.Loggingを使ってよいか？
trait Logic extends Logging {
  def execute(sparkSession: SparkSession): Unit
}
