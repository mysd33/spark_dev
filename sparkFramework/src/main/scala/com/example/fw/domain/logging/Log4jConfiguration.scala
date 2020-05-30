package com.example.fw.domain.logging

import java.io.InputStream

import com.example.fw.domain.const.FWConst
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.internal.Logging
import com.example.fw.domain.utils.Using._

/**
 *
 * Log4jの設定ファイル（log4j.properities)を動的に適用するオブジェクト
 *
 * configureメソッドを実行することで、
 * Sparkのデフォルトのlog4j-default.propertiesを適用する代わりに
 * プロファイルに応じた「log4j-(プロファイル名).properties」を上書き適用することができる。
 * application-XXX.properties(XXXはプロファイル名)に下記のような設定すると
 * {{{
 *   #log4j properties overwrite mode
 *   log4j.overwrite=true
 *   #log4j properties directory path
 *   log4j.properties.dir=/com/example/
 * }}}
 *
 * 以下のファイルパス形式のlog4j.propertiesを適用する。
 * log4j.overwrite=trueの場合、
 * {{{
 * （log4j.properties.dirプロパティの値）+ "log4j-" + (active.profileプロパティの値） + ".properties"
 * }}}
 *
 * 上記のapplication-XXX.properties(XXXはプロファイル名)の設定内容の例では、
 * プロファイル( JVM -Dオプションや環境変数「ACTIVE_PROFILE」）が「dev」の場合、
 * 「/com/example/log4j-dev.properties」が適用され
 * ソースフォルダの「src/main/resources/com/example/log4j-prod.properties」が適用される。*
 * また、プロファイルを「ut」に切り替えて指定すると、
 * ソースフォルダの「src/test/resources/com/example/log4j-ut.properties」が適用される。
 *
 * Databricksの場合、通常は、以下のサイトの推奨手順の通り、initスクリプトで上書きする手順となっているため使用しないこと。
 * （log4j.overwrite=falseとし無効化）
 * このため、本クラスの利用は、開発端末やCIサーバなどの開発時のユーティリティと主とする。
 * @see https://docs.microsoft.com/ja-jp/azure/databricks/kb/clusters/overwrite-log4j-logs
 *
 * また、Azure Monitorへのログ転送の場合は、独自のAppenderに切り替える以下の手順に従い、本クラスを使用してlog4j.propertiesを上書きする。
 * この際、com.microsoft.pnp.logging.Log4jConfigurationクラスの代わりに、本クラスを使用して上書きする。
 *
 * @see https://docs.microsoft.com/ja-jp/azure/architecture/databricks-monitoring/application-logs
 *
 *
 */
object Log4jConfiguration extends Logging {

  val LOG4J_PROPERTIES_FILE_PREFIX = "log4j-"

  val LOG4J_PROPERTIES_FILE_EXTENSION = ".properties"

  /**
   * application-XXX(XXXはプロファイル).propertiesでディレクトリ指定した配下のlog4j.propertiesを適用する
   */
  def configure(): Unit = {
    val isOverwrite = ResourceBundleManager.get(FWConst.LOG4J_OVERWRITE)
    if (isOverwrite != null && isOverwrite.toBoolean) {
      //プロパティ値とプロファイル値に基づくファイルパスを取得
      val log4jPropertiesDirPath = ResourceBundleManager.get(FWConst.LOG4J_PROPERTIES_DIR_KEY)
      val log4jPropertiesFilePath = log4jPropertiesDirPath +
        LOG4J_PROPERTIES_FILE_PREFIX + ResourceBundleManager.getActiveProfile() + LOG4J_PROPERTIES_FILE_EXTENSION
      configure(log4jPropertiesFilePath)
    }
  }

  /**
   * 指定したファイルパスのlog4j.propertiesを適用する
   *
   * @param configFilename log4j.propertiesのファイルパス
   */
  private def configure(configFilename: String): Unit = {
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
