package com.example.logic

import com.example.fw.domain.logic.Logic
import org.apache.spark.sql.SparkSession

class EmptyLogic extends Logic {
  override def execute(sparkSession: SparkSession): Unit = {
    println("emptylogic")
  }
}
