package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.utils.ResourceBundleManager
import com.example.fw.infra.dataaccess.StandardSparkDataFileReaderWriter
import com.example.fw.test.SparkTestFunSuite
import com.example.sample.common.logic.SampleSharedLogic
import com.example.sample.common.rule.PersonRule
import com.example.sample.common.entity.Person
import org.mockito.Mockito._
import org.scalatestplus.mockito._
import org.mockito.ArgumentMatchers._

class UTDemoDatasetBLogicTest extends SparkTestFunSuite with MockitoSugar {
  test("UTDemoDatasetBLogicTest.process") {
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

    //Mock
    //org.scalatestplus.mockito.MockitoSugerによりScalaTestでMockitoを利用
    //http://www.scalatest.org/user_guide/testing_with_mock_objects#mockito
    //http://tkawachi.github.io/blog/2013/08/26/mockito-scala/
    //共通処理クラスのMock
    val sampleSharedLogicMock = mock[SampleSharedLogic]
    //Mockの戻り値の設定
    when(sampleSharedLogicMock.execute(any())).thenReturn(inputDs)
    //ビジネスルールクラスのMock Serializableで作成
    val personRuleMock = mock[PersonRule](withSettings().serializable())
    //Mockの戻り値の設定
    when(personRuleMock.calcAge(any())).thenReturn(20)

    //テスト対象クラス
    val sut = new UTDemoDatasetBLogic(
      new DataFileReaderWriter with StandardSparkDataFileReaderWriter) {
      //overrideしてMockに置き換え
      override val sampleSharedLogic: SampleSharedLogic = sampleSharedLogicMock
      override val personRule: PersonRule = personRuleMock
    }
    //テスト対象のprocessメソッドを実行し期待値どおりか検証
    assertDataset(expected) {
      sut.process(inputDs, sparkSession)
    }
    //Mockが正しく呼ばれたことの検証
    //本当は正しい値が渡されたここのチェックができるとなおよい
    verify(sampleSharedLogicMock).execute(any())
    //Sparkの場合、ビジネスルール等、Spark APIのメソッド内の（mapメソッド等）でラムダ式で
    //実行されると、シリアライザブルされたリモートオブジェクトが実行されるためか
    //Mockが実行された記録されずテストNGになってしまうのでverifyは実施しない
    //verify(personRuleMock).calcAge(any())
  }

}
