package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.utils.ResourceBundleManager
import com.example.fw.infra.dataaccess.StandardSparkDataModelReaderWriter
import com.example.fw.test.SparkTestFunSuite
import com.example.sample.common.entity.Person

/**
 * AP基盤のテストフレームワークを利用したテストコードの例
 */
class SampleDataSetBLogic3Test extends SparkTestFunSuite {
  test("SampleDatasetBLogic3.process") {
    println("active.profile:" + ResourceBundleManager.getActiveProfile())
    import sparkSession.implicits._
    //入力
    val inputDs = Seq(
      Person("Michael", None),
      Person("Andy", Some(30)),
      Person("Justion", Some(19))).toDS()
    //期待値
    val expected = Seq(
      Person("Michael", None),
      Person("Andy", Some(30)),
      Person("Justion", Some(19))).toDS()
    //テスト対象クラス
    val sut = new SampleDataSetBLogic3(new DataModelReaderWriter with StandardSparkDataModelReaderWriter)

    //テスト実行し期待値どおりか検証
    assertDataset(expected) {
      sut.process(inputDs, sparkSession)
    }
  }
}
