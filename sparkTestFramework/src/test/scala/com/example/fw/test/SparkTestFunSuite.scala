package com.example.fw.test

import com.example.fw.app.StandardSparkSessionManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{Assertion, BeforeAndAfter, BeforeAndAfterAll}

/**
 * 単体テスト用基底テストクラス
 *
 */
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

  /**
   * RDDの値を比較する
   * @param expected 期待値のRDD
   * @param actual 実際のRDD
   * @tparam T RDDの型パラメータ
   * @return Assertionの結果
   */
  protected def assertRDD[T](expected: RDD[T])(actual: RDD[T]): Assertion = {
    AssertUtils.assertRDD(expected)(actual)
  }

  /**
   * Datasetの値を比較する
   * @param expected 期待値のDataset
   * @param actual 実際のDataset
   * @tparam T Datasetの型パラメータ
   * @return Assertionの結果
   */
  protected def assertDataset[T](expected: Dataset[T])(actual: Dataset[T]): Assertion = {
    AssertUtils.assertDataset(expected)(actual)
  }
}
