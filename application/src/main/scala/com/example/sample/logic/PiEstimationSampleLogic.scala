package com.example.sample.logic

import com.example.fw.domain.logic.Logic
import org.apache.spark.sql.SparkSession

/**
 * AP基盤を使ったサンプル
 *
 * Logicトレイトを実装した簡単な円周率計算サンプル
 */
class PiEstimationSampleLogic extends Logic {
  override def execute(sparkSession: SparkSession): Unit = {
    val sc = sparkSession.sparkContext

    val NUM_SAMPLES = 1000
    val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
      val x = math.random
      val y = math.random
      x * x + y * y < 1
    }.count()
    println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")

  }
}
