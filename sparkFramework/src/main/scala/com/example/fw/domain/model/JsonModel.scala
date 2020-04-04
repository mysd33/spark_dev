package com.example.fw.domain.model

import org.apache.spark.sql.Row

case class JsonModel(path: String) extends DataFile[Row] {
  require(path != null && path.length > 0)
  override val filePath: String = path
}
