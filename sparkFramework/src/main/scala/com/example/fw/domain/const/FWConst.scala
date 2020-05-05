package com.example.fw.domain.const

/**
 * フレームワークが使用する引数
 */
object FWConst {
  /**
   * APが扱うファイルの最上位のディレクトリを表すプロパティキー=basepath
   */
  val BASE_PATH_KEY = "basepath"
  /**
   * ファイルのデフォルトエンコーディング名=UTF-8
   */
  val DEFAULT_ENCODING = "UTF-8"
  /**
   * MultiFormatCsvModelが扱うデフォルト区切り文字 = NUL（\\u0000）
   */
  val DEFAULT_MULTIFORMAT_CSV_DELIMITER = "\u0000"

  /**
   * CsvModelが扱うデフォルト区切り文字=カンマ(,)
   */
  val DEFAULT_CSV_DELIMITER = ","
}
