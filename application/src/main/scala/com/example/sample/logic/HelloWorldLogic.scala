package com.example.sample.logic

import com.example.fw.domain.logic.Logic
import org.apache.spark.sql.SparkSession

/**
 * AP基盤を使ったサンプル
 *
 * もっとも簡単なHelloWorldサンプル
 */
class HelloWorldLogic extends Logic {
  override def execute(sparkSession: SparkSession): Unit = {
    println("Hello World")
  }
}
