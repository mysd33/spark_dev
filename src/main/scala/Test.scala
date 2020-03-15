import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    println("Spark Test")
    println(spark.range(100).count())

    //DBUtils
    val dbutils = com.databricks.service.DBUtils
    println("DBUtil Test")
    println(dbutils.fs.ls("dbfs:/"))
    //println(dbutils.secrets.listScopes())
  }
}