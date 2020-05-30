package com.example.fw.domain.logging

import java.io.InputStream

import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.internal.Logging
import com.example.fw.domain.utils.Using._

/**
 *
 * Log4jの設定ファイル（log4j.properities)を動的に適用するオブジェクト
 *
 */
object Log4jConfiguration extends Logging {
  private val LOG4J_PROPERTIES_DIR_KEY = "log4j.properties.dir"

  val LOG4J_PROPERTIES_FILE_PREFIX = "log4j-"

  val LOG4J_PROPERTIES_FILE_EXTENSION = ".properties"

  /**
   * application.propertiesでディレクトリ指定した配下のlog4j.propertiesを適用する。
   *
   * application.properties(またはapplication-xxx.properties）の設定内容によって
   * 以下ののファイルパス形式のlog4j.propertiesを適用する。
   *
   * （log4j.properties.dirプロパティの値）+ "log4j-" + (active.profileプロパティの値） + ".properties"
   *
   *
   * {{{
   *   #active profile( JVM -D option or Enviroment Vairoable "ACTIVE_PROFILE")
   *   active.profiles=prod
   *   #log4j properties directory path
   *   log4j.properties.dir=/com/example/
   * }}}
   *
   * この例の場合、「/com/example/log4j-prod.properties」が呼び出されるので、
   * ソースフォルダの「src/main/resources/com/example/log4j-prod.properties」が適用される。
   *
   * また、例えば、JVM起動時に-Dactive.profiles=ut（または環境変数ACTIVE_PROFILE=ut）と指定すると、
   * ソースフォルダの「src/test/resources/com/example/log4j-ut.properties」が適用される。
   *
   *
   */
  def configure(): Unit = {
    //プロパティ値とプロファイル値に基づくファイルパスを取得
    val log4jPropertiesDirPath = ResourceBundleManager.get(LOG4J_PROPERTIES_DIR_KEY)
    val log4jPropertiesFilePath = log4jPropertiesDirPath +
      LOG4J_PROPERTIES_FILE_PREFIX + ResourceBundleManager.getActiveProfile() + LOG4J_PROPERTIES_FILE_EXTENSION
    configure(log4jPropertiesFilePath)
  }

  /**
   * 指定したファイルパスのlog4j.propertiesを適用する
   *
   * @param configFilename log4j.propertiesのファイルパス
   */
  def configure(configFilename: String): Unit = {
    using(getClass.getResourceAsStream(configFilename)) {
      stream => {
        Log4jConfiguration.configure(stream)
        logInfo(s"Application Framework apply: ${configFilename}")
      }
    }
  }

  /**
   * log4j.propertiesを適用する
   *
   * @param inputStream log4j.propertiesのInputSteam
   */
  private def configure(inputStream: InputStream): Unit = {
    PropertyConfigurator.configure(inputStream);
  }
}
