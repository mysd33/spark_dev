import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object WordCount{

  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    spark.sparkContext.addJar("./target/scala-2.11/databricks_dev.jar")

    val textFile = sc.textFile("dbfs:/databricks-datasets/samples/docs/README.md")
    val words = textFile.flatMap(line => line.split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey((a,b) => a+b)

    println(wordCounts)

    //wordCounts.saveAsTextFile("/・・・/")
  }
}
