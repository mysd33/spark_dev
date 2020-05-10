import org.apache.spark.sql.SparkSession

/**
 * もっとも簡単なDataFrameのサンプル。
 *
 * AP基盤機能も使っていない。
 */
object SparkSQLExample {
  def main(args: Array[String]): Unit = {
    val file = "C:\\temp\\person.json"
    val master = "local[*]"
    val logLevel = "WARN"
    val spark = SparkSession.builder.master(master)
      .appName(this.getClass().toString()).getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)

    import spark.implicits._
    val df = spark.read.json(file).cache()
    df.show()
    df.printSchema()
    df.select("name").show()
    df.filter($"age" > 20).show()
    df.groupBy("age").count().show()

    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

  }
}
