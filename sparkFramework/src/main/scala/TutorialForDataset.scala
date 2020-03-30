import org.apache.spark.sql.SparkSession

object TutorialForDataset {
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
    val df = spark.read.json(file)
    df.show()

    //Creating Datasets
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect().foreach(printf("%s,", _))

    //TODO: Interoperating with RDDs
    // http://spark.apache.org/docs/latest/sql-getting-started.html#interoperating-with-rdds

    //TODO: UDF
    // http://spark.apache.org/docs/latest/sql-getting-started.html#untyped-user-defined-aggregate-functions
  }
}

case class Person(name: String, age: Long)