package com.example.fw.infra.dataaccess.impl

import com.databricks.spark.xml._
import com.example.fw.domain.model.XmlModel
import org.apache.spark.sql._

//TODO: spark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しないので本番開発では使用しない
class XmlReaderWriter {
  //spark-xmlを利用
  //https://github.com/databricks/spark-xml/blob/master/README.md

  def readToDf(inputFile: XmlModel[Row], sparkSession: SparkSession): DataFrame = {
    val reader = inputFile.rowTag match {
      case Some(rowTag) => sparkSession.read.option("rowTag", rowTag)
      case _ => sparkSession.read
    }
    val reader2 = inputFile.schema match {
      case Some(schema) => reader.schema(schema)
      case _ => reader
    }
    val reader3 = inputFile.encoding match {
      case Some(encoding) => reader2.option("encoding", encoding)
      case _ => reader2
    }
    reader3.xml(inputFile.filePath)
  }

  def writeFromDsDf[T](ds: Dataset[T], outputFile: XmlModel[T], saveMode: SaveMode): Unit = {
    val writer = ds.write.mode(saveMode)
    val writer2 = outputFile.rootTag match {
      case Some(rootTag) => writer.option("rootTag", rootTag)
      case _ => writer
    }
    val writer3 = outputFile.rowTag match {
      case Some(rowTag) => writer2.option("rowTag", rowTag)
      case _ => writer2
    }
    writer3.xml(outputFile.filePath)
  }

}
