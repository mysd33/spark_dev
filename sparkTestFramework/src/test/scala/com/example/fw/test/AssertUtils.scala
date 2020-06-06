package com.example.fw.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.scalatest.Assertion
import org.scalatest.Assertions._

object AssertUtils {
  /**
   * RDDの値を比較する
   * @param expected 期待値のRDD
   * @param actual 実際のRDD
   * @tparam T RDDの型パラメータ
   * @return Assertionの結果
   */
  def assertRDD[T](expected: RDD[T])(actual: RDD[T]): Assertion = {
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

  /**
   * Datasetの値を比較する
   * @param expected 期待値のDataset
   * @param actual 実際のDataset
   * @tparam T Datasetの型パラメータ
   * @return Assertionの結果
   */
  def assertDataset[T](expected: Dataset[T])(actual: Dataset[T]): Assertion = {
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
