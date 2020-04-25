package com.example.fw.test

import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{Assertion, BeforeAndAfter, BeforeAndAfterAll}

abstract class DatasetBLogicFunSuite extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterAll {
  lazy protected val sparkSession: SparkSession = {
    val clusterMode = ResourceBundleManager.get("clustermode")
    val logLevel = ResourceBundleManager.get("loglevel")
    val spark = SparkSession.builder()
      .master(clusterMode)
      //TODO: Config設定の検討
      //https://spark.apache.org/docs/latest/configuration.html
      //.config("key", "value")
      .getOrCreate()
    val sc = spark.sparkContext
    //TODO:ログが多いのでオフしている。log4j.propertiesで設定できるようにするなど検討
    sc.setLogLevel(logLevel)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    spark
  }

  override protected def beforeAll(): Unit = {
  }

  override protected def afterAll(): Unit = {
    //sparkSession.stop()
  }

  protected def assertDataset[T](expected: Dataset[T])(actual: Dataset[T]): Assertion = {
    if (expected == null && actual == null) {
      succeed
    } else {
      val expectedArray = expected.collect()
      val actualArray = actual.collect()
      assert(expectedArray.length == actualArray.length)

      expectedArray.zip(actualArray)
        .foreach(pair => {
          assert(pair._1 == pair._2)
        })
      succeed
    }

  }
}
