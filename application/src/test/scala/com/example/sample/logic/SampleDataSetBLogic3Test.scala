package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.utils.ResourceBundleManager
import com.example.fw.infra.dataaccess.StandardSparkDataFileReaderWriter
import com.example.fw.test.DatasetBLogicFunSuite
import com.example.sample.model.Person

class SampleDataSetBLogic3Test extends DatasetBLogicFunSuite[Person, Person] {
  val sut = new SampleDataSetBLogic3(new DataFileReaderWriter with StandardSparkDataFileReaderWriter)

  test("SampleDatasetBLogic3.process") {
    println("active.profile:" + ResourceBundleManager.getActiveProfile())
    import sparkSession.implicits._
    val inputDs = Seq(
      Person("Michael", None),
      Person("Andy", Option(30)),
      Person("Justion", Option(19))).toDS()
    val expected = Seq(
      Person("Michael", None),
      Person("Andy", Option(30)),
      Person("Justion", Option(19))).toDS()
    assertDataset(expected){
      sut.process(inputDs, sparkSession)
    }
  }
}
