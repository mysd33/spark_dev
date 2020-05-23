package com.example.fw.test

import com.example.fw.app.StandardSparkSessionManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{Assertion, BeforeAndAfter, BeforeAndAfterAll}

abstract class SparkTestFunSuite extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterAll {
  lazy protected val sparkSession: SparkSession = {
    StandardSparkSessionManager.createSparkSession(getClass().getName)
  }
  lazy protected val sparkContext = sparkSession.sparkContext

  override protected def beforeAll(): Unit = {
  }

  override protected def afterAll(): Unit = {
    //sparkSession.stop()
  }

  protected def assertRDD[T](expected: RDD[T])(actual: RDD[T]): Assertion = {
    if (expected == null && actual == null) {
      succeed
    } else {
      val expectedArray = expected.collect()
      val actualArray = actual.collect()
      expectedArray.zip(actualArray)
        .foreach(pair => {
          assert(pair._1 == pair._2)
        })
      succeed
    }
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
