package com.example.fw.domain.utils

import org.apache.spark.sql.types.StructType

/**
 * Optionクラスに関する暗黙の型変換を行うユーティリティクラス
 *
 * DataFileの引数がOption型の場合に、本クラスをインポートするとことで、
 * Option型で作成不要になりコードが短くなる
 * {{{
 *   import com.example.fw.domain.utils.OptionImplicit._
 * }}}
 *
 */
object OptionImplicit {
  /**
   * String型をOption[String]型に暗黙の型変換している
   * @param str 変換対象の文字列
   * @return 変換後のOption
   */
  implicit def string2option(str: String):Option[String] = Option(str)

  /**
   *
   * StructType型をOption[StructType]型に暗黙の型変換している
   * @param structType 変換対象のStructType
   * @return 変換後のOption
   */
  implicit def structType2option(structType: StructType):Option[StructType] = Option(structType)
}
