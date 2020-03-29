package com.example.samle.businesslogic

import com.example.fw.Logic
import org.apache.spark.sql.SparkSession

class SampleLogic extends Logic {
  override def execute(sparkSession: SparkSession): Unit = {
    println("Hello World")
  }
}
