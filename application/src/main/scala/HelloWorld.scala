import org.apache.spark.{SparkConf, SparkContext}

/**
 * もっとも簡単なHelloWordサンプル。
 *
 * AP基盤機能も使っていない。
 */
object HelloWorld {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local").
      setAppName("LearnScalaSpark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val helloWorldString = "Hello World!"
    print(helloWorldString)

  }
}
