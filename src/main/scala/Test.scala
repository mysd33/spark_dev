import com.example.sample.utils.SampleUtils
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    println("Spark Test")
    println(spark.range(100).count())

    //部品の呼び出し
    //workspace.xmlにcomponent要素：PropertiesComponentに
    // <property name="dynamic.classpath" value="true" />を追加しないと
    // Command line is too long が出る
    new SampleUtils().helloWorld()

    //DBUtils
    val dbutils = com.databricks.service.DBUtils
    println("DBUtil Test")
    println(dbutils.fs.ls("dbfs:/"))
    //println(dbutils.secrets.listScopes())
  }
}