package com.example.fw.domain.model

import org.apache.spark.sql.Row

case class XmlModel(path: String) extends DataFile[Row] {
  //TODO:XML形式にするのはどこでやる？
  override val filePath: String = path
}
