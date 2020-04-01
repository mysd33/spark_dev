package com.example.sample.logic

import com.example.fw.domain.logic.Logic
import org.apache.spark.sql.SparkSession

class SampleLogic extends Logic {
  override def execute(sparkSession: SparkSession): Unit = {
    println("Hello World")
  }
}
