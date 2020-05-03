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
    val reader = if (xmlFile.rowTag == null) {
      sparkSession.read
    } else {
      sparkSession.read.option("rowTag", xmlFile.rowTag)
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
    val writer2 = if (xmlFile.rootTag == null) {
      writer
    } else {
      writer.option("rootTag", xmlFile.rootTag)
    }
    val writer3 = if (xmlFile.rowTag == null) {
      writer2
    } else {
      writer2.option("rowTag", xmlFile.rowTag)
    }
    writer3.xml(outputFile.filePath)
  }

}
