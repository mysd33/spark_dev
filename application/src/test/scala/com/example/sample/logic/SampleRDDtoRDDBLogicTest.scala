package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.utils.ResourceBundleManager
import com.example.fw.infra.dataaccess.StandardSparkDataModelReaderWriter
import com.example.fw.test.SparkTestFunSuite
import com.example.sample.common.entity.Person

/**
 * AP基盤のテストフレームワークを利用したテストコードの例
 */
class SampleRDDtoRDDBLogicTest extends SparkTestFunSuite {
  test("SampleRDDtoRDDBLogicTest.process") {
    println("active.profile:" + ResourceBundleManager.getActiveProfile())
    //入力
    val inputs = sparkContext.parallelize(
      Seq("hoge hoge fuga hoge", "fuga")) :: Nil
    //期待値
    val expected = sparkContext.parallelize(
      Seq(("hoge", 3), ("fuga", 2)))
      .sortByKey()   //sortして順番をそろえておく
    //テスト対象クラス
    val sut = new SampleRDDtoRDDBLogic(new DataModelReaderWriter with StandardSparkDataModelReaderWriter)

    //テスト実行し期待値どおりか検証
    assertRDD(expected) {
      sut.process(inputs, sparkSession)
        .sortByKey()   //sortして順番をそろえておく
    }

  }
}
