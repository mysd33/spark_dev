package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.logic.DataFrameBLogic
import com.example.fw.domain.model.{JsonModel, ParquetModel}
import com.example.sample.common.entity.Person
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * AP基盤を使ったサンプル
 *
 * DataFrameBLogicクラスを継承し、Jsonファイルを読み込んでParquetファイルを出力するサンプル
 *
 * @param dataModelReaderWriter Logicクラスが使用するDataModelReaderWriter
 */
class SampleDataFrameBLogic(dataModelReaderWriter: DataModelReaderWriter)
  extends DataFrameBLogic(dataModelReaderWriter) {
  //JSONファイルの読み込みの例
  override val inputModels = JsonModel[Row]("person.json") :: Nil
  //Parquetファイルの書き込みの例
  override val outputModels = ParquetModel[Row]("person.parquet") :: Nil

  override def process(dfList: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame] = {
    //DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    import sparkSession.implicits._
    val ds = dfList(0).as[Person]
    ds.show()
    ds.toDF() :: Nil
  }
}
