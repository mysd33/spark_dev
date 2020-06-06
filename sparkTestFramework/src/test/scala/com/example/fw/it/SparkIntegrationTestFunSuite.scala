package com.example.fw.it

import java.io.File

import com.example.fw.app.StandardSparkSessionManager
import com.example.fw.domain.const.FWConst
import com.example.fw.domain.utils.ResourceBundleManager
import com.example.fw.test.AssertUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{Assertion, BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.JavaConversions._

/**
 * 結合テスト用基底テストクラス
 */
abstract class SparkIntegrationTestFunSuite extends AnyFunSuite with BeforeAndAfter with BeforeAndAfterAll {
  /**
   * 入力テストデータ
   */
  protected val inputTestDataDirPath: String
  /**
   * 結合テストの作業ディレクトリ
   *
   * 入力テストデータをこのディレクトリにコピーしてテストを実行する
   */
  protected val workingDir: File = new File(ResourceBundleManager.get(FWConst.BASE_PATH_KEY))


  override protected def beforeAll(): Unit = {
    val inputTestDataDir = new File(inputTestDataDirPath)
    //TODO: 配下のディレクトリをまるっとコピー
    //テストデータをワーキングディレクトリにコピー
    val inputTestFiles = FileUtils.listFiles(inputTestDataDir, null, false)

    inputTestFiles.seq.foreach(f => FileUtils.copyFileToDirectory(f, workingDir))
  }

  override protected def afterAll(): Unit = {
    //TODO:ワーキングディレクトリを削除はテストコードごと？
    FileUtils.deleteDirectory(workingDir)
  }

  /**
   *
   * @param logicClassFQDN 実行するビジネスロジッククラスのFQDN
   * @param methodArgs 実行するビジネスロジッククラスへ渡す起動パラメータ
   * @param assertionLogic assertion処理を実装
   */
  protected def runJob(logicClassFQDN: String, methodArgs: Array[String] = null)(assertionLogic: => Unit): Unit = {
    //SparkでBLogicクラスを実行
    StandardSparkSessionManager.run(logicClassFQDN, methodArgs)
    //実装したアサーション処理を使って実行結果確認
    assertionLogic
  }

  /**
   * RDDの値を比較する
   *
   * @param expected 期待値のRDD
   * @param actual   実際のRDD
   * @tparam T RDDの型パラメータ
   * @return Assertionの結果
   */
  protected def assertRDD[T](expected: RDD[T])(actual: RDD[T]): Assertion = {
    AssertUtils.assertRDD(expected)(actual)
  }

  /**
   * Datasetの値を比較する
   *
   * @param expected 期待値のDataset
   * @param actual   実際のDataset
   * @tparam T Datasetの型パラメータ
   * @return Assertionの結果
   */
  protected def assertDataset[T](expected: Dataset[T])(actual: Dataset[T]): Assertion = {
    AssertUtils.assertDataset(expected)(actual)
  }
}
