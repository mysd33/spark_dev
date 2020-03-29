package com.example.fw

import org.apache.spark.sql.SparkSession

trait Logic {
  def execute(sparkSession: SparkSession): Unit
}
