import org.apache.spark.sql.SparkSession

object Tutorial {
  def main(args: Array[String]): Unit = {
    val file = "C:\\temp\\README.md"
    val master = "local[*]"
    val logLevel = "WARN"
    val spark = SparkSession.builder.master(master)
      .appName(this.getClass().toString()).getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)
    val textFile = spark.read.textFile(file)
    println(textFile.count())
    println(textFile.first())
    val linesWithSpark = textFile.filter(line => line.contains("Spark"))
    println(linesWithSpark.count())

    import spark.implicits._
    val wordCounts = textFile.flatMap(line => line.split(" ")).groupByKey(identity).count()
    wordCounts.collect().foreach(println)
    spark.stop()
  }
}