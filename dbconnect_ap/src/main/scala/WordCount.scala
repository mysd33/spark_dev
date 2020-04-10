import org.apache.spark.sql.SparkSession

object WordCount{

  def main(args:Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    sc.addJar("./target/scala-2.11/databricks_dev_2.11-0.1.jar")

    //DBUtils
    val dbutils = com.databricks.service.DBUtils
    dbutils.fs.rm("/temp/" , true)

    val textFile = sc.textFile("dbfs:/databricks-datasets/samples/docs/README.md")
    val words = textFile.flatMap(line => line.split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey((a,b) => a+b)
    //println(wordCounts)
    //TODO:collectを使うとエラー
    //wordCounts.collect().foreach(println)

    wordCounts.saveAsTextFile("dbfs:/temp/")
    println(dbutils.fs.ls("/temp/"))
    //spark.stop()
  }
}
