package com.example.sample.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.DataFrameBLogic
import com.example.fw.domain.model.{CsvModel, DataFile, XmlModel}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class SampleXMLDatasetBLogic(dataFileReaderWriter: DataFileReaderWriter)
  extends DataFrameBLogic(dataFileReaderWriter) {
  override val inputFiles: Seq[DataFile[Row]] =
    XmlModel[Row](path = "xml/books.xml", rowTag = "book",
      schm = StructType(Array(
        StructField("_id", StringType, nullable = true),
        StructField("author", StringType, nullable = true),
        StructField("description", StringType, nullable = true),
        StructField("genre", StringType, nullable = true),
        StructField("price", DoubleType, nullable = true),
        StructField("publish_date", StringType, nullable = true),
        StructField("title", StringType, nullable = true)))
    ) :: Nil

  override val outputFiles: Seq[DataFile[Row]] =
    XmlModel[Row](path = "xml/newbooks.xml", rootTag = "books", rowTag = "book"
    ) :: CsvModel[Row]("xml/newbooks.csv"
    ) :: Nil

  override def process(inputs: Seq[DataFrame], sparkSession: SparkSession): Seq[DataFrame] = {
    val df = inputs(0)
    val result = df.select("author", "_id")

    val cached = result.cache()
    cached :: cached :: Nil
  }

}
