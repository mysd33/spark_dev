package app.businesslogic

import com.example.fw.domain.logic.Logic
import org.apache.spark.sql.SparkSession

class PiEstimationLogic extends Logic {
  override def execute(sparkSession: SparkSession): Unit = {
    //TODO: 依存関係のjarの追加操作を記載しないとエラー
    val sc = sparkSession.sparkContext
    sc.addJar("./target/scala-2.11/sparkframework_2.11-0.1.jar")
    sc.addJar("./target/scala-2.11/databricks_dev_2.11-0.1.jar")

    val NUM_SAMPLES = 1000
    val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
      val x = math.random
      val y = math.random
      x * x + y * y < 1
    }.count()
    println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")

  }
}
