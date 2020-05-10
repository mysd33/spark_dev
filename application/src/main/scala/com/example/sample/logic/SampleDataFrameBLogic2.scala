package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DataFrameBLogic
import com.example.fw.domain.model.{CsvModel, ParquetModel}
import com.example.sample.common.entity.Person
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * AP基盤を使ったサンプル
 *
 * DataFrameBLogicクラスを継承し、ヘッダ付きCSVファイルを読み込んでParquetファイルを出力するサンプル
 *
 * @param dataFileReaderWriter Logicクラスが使用するDataFileReaderWriter
 */
class SampleDataFrameBLogic2(dataFileReaderWriter: DataFileReaderWriter)
  extends DataFrameBLogic(dataFileReaderWriter) {
  //ヘッダ付きのCSVファイルの読み込みの例
  override val inputFiles = CsvModel[Row]("person.csv", hasHeader = true) :: Nil
  //Parquetファイルの書き込みの例
  override val outputFiles = ParquetModel[Row]("person.parquet") :: Nil

  override def process(inputs: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame] = {
    //DataSetで扱おうとするとimport文が必要なのでsparkSessionが引数に必要
    import sparkSession.implicits._
    val ds = inputs(0).as[Person]
    ds.show()
    ds.toDF() :: Nil
  }
}
