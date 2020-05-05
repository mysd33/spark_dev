package com.example.fw.infra.dataaccess.impl

import com.databricks.spark.xml._
import com.example.fw.domain.model.XmlModel
import org.apache.spark.sql._

import com.example.fw.infra.dataaccess.impl.ReaderMethodBuilder._
import com.example.fw.infra.dataaccess.impl.WriterMethodBuilder._

//TODO: spark-xmlの依存jarをすべてDatabricksクラスタにインストールしないと動作しないので本番開発では使用しない
class XmlReaderWriter {
  //spark-xmlを利用
  //https://github.com/databricks/spark-xml/blob/master/README.md
  //Optionの実装
  //https://github.com/databricks/spark-xml#features

  def readToDf(inputFile: XmlModel[Row], sparkSession: SparkSession): DataFrame = {
    val reader = inputFile.rowTag match {
      case Some(rowTag) => sparkSession.read.option("rowTag", rowTag)
      case _ => sparkSession.read
    }
    val reader2 = inputFile.encoding match {
      //encodingではなくcharset
      case Some(encoding) => reader.option("charset", encoding)
      case _ => reader
    }
    reader2
      //暗黙の型変換でメソッド拡張
      .buildSchema(inputFile)
      .xml(inputFile.absolutePath)
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
    writer3
      //暗黙の型変換でメソッド拡張
      .buildOptionCompression(outputFile)
      .xml(outputFile.absolutePath)
  }

}
