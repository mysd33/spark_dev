package com.example.fw.infra.dataaccess.impl

import com.example.fw.domain.dataaccess.DataFileReaderWriterImpl
import com.example.fw.domain.model.{DataFile, XmlModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import com.databricks.spark.xml._

class XmlReaderWriter extends DataFileReaderWriterImpl {
  //spark-xmlを利用
  //https://github.com/databricks/spark-xml/blob/master/README.md

  override def readToDf(inputFile: DataFile[Row], sparkSession: SparkSession): DataFrame = {
    //TODO: XmlModel型のチェック
    val xmlFile = inputFile.asInstanceOf[XmlModel[Row]]
    val reader = xmlFile.rowTag match {
      case Some(rowTag) => sparkSession.read.option("rowTag", rowTag)
      case _ => sparkSession.read
    }
    val reader2 = xmlFile.schema match {
      case Some(schema) => reader.schema(schema)
      case _ => reader
    }
    val reader3 = xmlFile.encoding match {
      case Some(encoding) => reader2.option("encoding", encoding)
      case _ => reader2
    }
    reader3.xml(inputFile.filePath)
  }

  override def writeFromDsDf[T](ds: Dataset[T], outputFile: DataFile[T], saveMode: SaveMode): Unit = {
    //TODO: XmlModel型のチェック
    val xmlFile = outputFile.asInstanceOf[XmlModel[T]]

    val writer = ds.write.mode(saveMode)
    val writer2 = xmlFile.rootTag match {
      case Some(rootTag) => writer.option("rootTag", rootTag)
      case _ => writer
    }
    val writer3 = xmlFile.rowTag match {
      case Some(rowTag) => writer2.option("rowTag", rowTag)
      case _ => writer2
    }
    writer3.xml(outputFile.filePath)
  }

}
