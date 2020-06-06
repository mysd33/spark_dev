package com.example.sample.logic

import com.example.fw.it.SparkIntegrationTestFunSuite

/**
 * 結合テストコードの例
 */
class ITDemoDatasetBLogicTest extends SparkIntegrationTestFunSuite {
  //テストデータの格納されているフォルダを設定
  override val inputTestDataDirPath: String = "testdata/input"

  test("ITDemoDatasetBLogicTest") {
    runJob("com.example.sample.logic.SampleDataSetBLogic3") {
      //TODO: BLogicの結果ファイルをDataSetから取得してassertを実装
    }
  }

}
