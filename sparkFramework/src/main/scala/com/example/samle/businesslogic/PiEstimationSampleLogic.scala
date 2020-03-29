package com.example.samle.businesslogic

import com.example.fw.Logic
import org.apache.spark.sql.SparkSession

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
