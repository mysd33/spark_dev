package com.example.fw.domain.const

/**
 * フレームワークが使用する引数
 */
object FWConst {
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

////以降、application.propertiesに設定するキー
  /**
   * APが扱うファイルの最上位のディレクトリを表すプロパティキー=basepath
   */
  val BASE_PATH_KEY = "basepath"

  /**
   * Sparkのクラスタモードを表すプロパティキー
   */
  val CLUSTER_MODE_KEY = "clustermode"

  /**
   * log4j.propertiesの上書き機能を有効化することを表すプロパティキー
   */
  val LOG4J_OVERWRITE = "log4j.overwrite"

  /**
   * log4j.propertiesの上書き機能を有効にしたときのDirectoryパスを表すプロパティキー
   */
  val LOG4J_PROPERTIES_DIR_KEY = "log4j.properties.dir"
}
