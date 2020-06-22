package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.logic.DatasetBLogic2to1
import com.example.fw.domain.model.{DataModel, JsonModel, ParquetModel}
import com.example.sample.common.entity.{Person, PersonOther}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * AP基盤を使ったサンプル
 *
 * DatasetBLogic2to1クラスを継承し、Jsonファイルを２つ読み込んでParquetファイルを１つ出力するサンプル
 *
 * @param dataModelReaderWriter Logicクラスが使用するDataModelReaderWriter
 */
class SampleDatasetBLogic2(dataModelReaderWriter: DataModelReaderWriter)
  extends DatasetBLogic2to1[Person, PersonOther, Person](dataModelReaderWriter) {
  //1つ目のJsonファイルを読み込む例
  override val inputModel1: DataModel[Person] = JsonModel[Person]("person.json")
  //２つ目のJsonファイルを読み込む例
  override val inputModel2: DataModel[PersonOther] = JsonModel[PersonOther]("person.json")
  //Parquetファイルを書き込む例
  override val outputModel: DataModel[Person] = ParquetModel[Person]("person_union.parquet")

  override def process(ds1: Dataset[Person], ds2: Dataset[PersonOther], sparkSession: SparkSession): Dataset[Person] = {
    import sparkSession.implicits._
    //DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    val dsTemp = ds2.map(po => Person(po.name, po.age))
    //unionの例
    val ds3 = ds1.unionByName(dsTemp)
    ds3.show()
    ds3
  }
}
