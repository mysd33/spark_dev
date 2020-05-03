package com.example.fw.domain.utils

import org.apache.spark.sql.types.StructType

object OptionImplicit {
  implicit def string2option(str: String):Option[String] = Option(str)
  implicit def structType2option(structType: StructType):Option[StructType] = Option(structType)
}
