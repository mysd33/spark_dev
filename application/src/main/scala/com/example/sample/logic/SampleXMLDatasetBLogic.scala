package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataModelReaderWriter
import com.example.fw.domain.logic.DataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataModel, XmlModel}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.example.fw.domain.utils.OptionImplicit._


/**
 * AP基盤を使ったサンプル
 *
 * 事前にシェルで、1行1特定検診XMLで複数特定検診のXMLを連結したテキストファイルを
 * 読み込み、各タグごとに、CSVファイルに書き込みを試行した例
 *
 * spark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しない
 *
 * @deprecated このサンプルでは簡単だが、spark-xmlは、ネストした複雑なXMLデータ構造だと煩雑なコードになってしまうし、ファイルを読んで逐次動作させながらでないと実装が難しいので使わない
 *
 * @param dataModelReaderWriter Logicクラスが使用するDataModelReaderWriter
 */
class SampleXMLDatasetBLogic(dataModelReaderWriter: DataModelReaderWriter)
  extends DataFrameBLogic(dataModelReaderWriter) {
  //spark-xmlの機能でXMLファイルを読み込む例
  override val inputModels: Seq[DataModel[Row]] =
    XmlModel[Row](relativePath = "xml/books.xml", rowTag = "book",
      schema = StructType(Array(
        StructField("_id", StringType, nullable = true),
        StructField("author", StringType, nullable = true),
        StructField("description", StringType, nullable = true),
        StructField("genre", StringType, nullable = true),
        StructField("price", DoubleType, nullable = true),
        StructField("publish_date", StringType, nullable = true),
        StructField("title", StringType, nullable = true)))
    ) :: Nil

  //spark-xmlの機能でXMLファイルを書き込む例
  override val outputModels: Seq[DataModel[Row]] =
    XmlModel[Row](relativePath = "xml/newbooks.xml", rootTag = "books", rowTag = "book"
    ) :: CsvModel[Row]("xml/newbooks.csv"
    ) :: Nil

  override def process(dfList: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame] = {
    val df = dfList(0)
    val result = df.select("author", "_id")

    val cached = result.cache()
    cached :: cached :: Nil
  }

}
