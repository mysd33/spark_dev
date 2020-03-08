import org.apache.spark.sql.SparkSession

object PiEstimation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()

    //依存関係のjarの追加操作を記載しないとエラー
    spark.sparkContext.addJar("./target/scala-2.11/databricks_dev.jar")
    val sc = spark.sparkContext

    val NUM_SAMPLES = 1000
    val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
      val x = math.random
      val y = math.random
      x * x + y * y < 1
    }.count()
    println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")
  }
}
