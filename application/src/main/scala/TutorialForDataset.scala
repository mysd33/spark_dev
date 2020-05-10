import org.apache.spark.sql.SparkSession

/**
 * もっとも簡単なDatasetサンプル
 *
 * AP基盤機能も使っていない。
 */
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

  }
}

case class Person(name: String, age: Long)