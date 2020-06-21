package com.example.sample.logic

import com.example.fw.it.SparkIntegrationTestFunSuite
import com.example.sample.common.entity.Person

/**
 * 結合テストコードの例
 */
class ITDemoDatasetBLogicTest extends SparkIntegrationTestFunSuite {
  //テストデータの格納されているフォルダを設定
  override val inputTestDataDirPath: String = "testdata/input"

  test("ITDemoDatasetBLogicTest") {
    //テスト対象のFQDNを指定し、runJobメソッドを実行
    runJob("com.example.sample.logic.SampleDataSetBLogic3") {
      //ジョブのテスト結果によるassertの比較対象の実際のDataset（actual）を取得
      sparkSession => {
        import sparkSession.implicits._
        //TODO: 本当の期待値のデータセット（
        //val expected = Seq().….toDs()
        val expected = Seq(new Person("dummy", Option(20))).toDS()

        //期待値とテスト対象のジョブの実行結果をassert
        assertDataset(expected) {
          //TODO: 比較のため実行結果のファイルをSparkで読み込みデータセット取得
          //val actual = sparkSession.….toDs()
          val actual = Seq(new Person("dummy", Option(20))).toDS()
          actual
        }
      }
    }
  }
}