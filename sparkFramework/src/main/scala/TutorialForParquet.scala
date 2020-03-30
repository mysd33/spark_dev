import org.apache.spark.sql.{SaveMode, SparkSession}

object TutorialForParquet {
  def main(args: Array[String]): Unit = {
    val file = "C:\\temp\\person.json"
    val master = "local[*]"
    val logLevel = "WARN"

    //Starting Point: SparkSession
    val spark = SparkSession.builder.master(master)
      .appName(this.getClass().toString()).getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)
    import spark.implicits._

    //Creating DataFrames
    val personDF = spark.read.json(file)
    personDF.show()

    // Dataframe is saved as Parquet files
    val parquetFile = "C:\\temp\\people.parquet"
    //personDF.write.parquet(parquetFile)
    personDF.write.mode(SaveMode.Overwrite).parquet(parquetFile)

    // Read in the parquet file
    val parquetFileDF = spark.read.parquet(parquetFile)
    parquetFileDF.show()
    //create a temporary view
    parquetFileDF.createOrReplaceTempView("people")
    //SQL statements
    val nameDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    nameDF.map(attr => "Name: " + attr(0)).show()

    // Create a simple DataFrame
    val squaresDF = spark.sparkContext.makeRDD(1 to 5)
      .map(i => (i, i * i)).toDF("value", "square")
    val baseDir = "C:\\temp\\data\\test_table"
    squaresDF.write.mode(SaveMode.Overwrite).parquet(baseDir + "\\key=1")
    val cubesDF = spark.sparkContext.makeRDD(6 to 10)
      .map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.mode(SaveMode.Overwrite).parquet(baseDir + "\\key=2")

    //mergeSchema
    val mergedDF = spark.read.option("mergeSchema", "true").parquet(baseDir)
    mergedDF.printSchema()
    mergedDF.show()
  }
}
