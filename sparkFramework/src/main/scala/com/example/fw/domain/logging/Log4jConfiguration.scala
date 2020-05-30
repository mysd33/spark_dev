package com.example.fw.domain.logging

import java.io.InputStream

import com.example.fw.domain.const.FWConst
import com.example.fw.domain.utils.ResourceBundleManager
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.internal.Logging
import com.example.fw.domain.utils.Using._

/**
 * ロギング機能を提供するオブジェクト
 *
 * Log4jの設定ファイル（log4j.properties)をカスタマイズしたファイルで動的に上書き適用する。
 *
 * 本システムでは、Sparkの標準のロギング機能(org.apache.spark.internal.Logging)を使用してログを出力する。
 * {{{
 *    import org.apache.spark.internal.Logging
 *
 *    class A extends Logging {
 *       def method1(msg: String): Unit = {
 *          logTrace("Trace message")
 *          logDebug("Debug message")
 *          logInfo("Info message")
 *          logWarning("Warning message")
 *          logError("Error message")
 *       }
 *    }
 * }}}
 *
 *
 * 本クラスの「configure」メソッドを実行することで、
 * Sparkのデフォルトの「log4j-default.properties」を適用する代わりに
 * 開発者が記述した「log4j-XXX.properties」(XXXはプロファイル名)を上書き適用することができる。
 *
 * プロファイルは、プロパティ管理機能により、JVMの-Dactive.profile起動オプションまたは環境変数「ACTIVE_PROFILE」を使って実行環境によって
 * 設定を切り替えることができる。
 *
 * 「application-XXX.properties」(XXXはプロファイル名)に、下記のような設定する。
 * {{{
 *   #log4j properties overwrite mode
 *   log4j.overwrite=true
 *
 *   #log4j properties directory path
 *   log4j.properties.dir=/com/example/
 * }}}
 *
 *  - 上記の通り「log4j.overwrite=true」の場合、本機能が有効化され、以下のファイルパスのlog4j.propertiesをロードし上書き適用する。
 *    - （log4j.properties.dirプロパティで指定したディレクトリ）+ "log4j-" + (active.profileプロパティ値） + ".properties"」
 *  - 「log4j.overwrite=false」の場合は、本機能は有効化されず、Sparkのデフォルトのlog4jの設定が適用される。
 *
 * 上記のapplication-XXX.properties(XXXはプロファイル名)の設定内容の例
 *  - プロファイルが「dev」の場合
 *    - 「/com/example/log4j-dev.properties」が適用
 *      - ソースフォルダの「src/main/resources/com/example/log4j-dev.properties」
 *  - プロファイルが「ut」の場合
 *    - 「/com/example/log4j-ut.properties」が適用
 *      - ソースフォルダの「src/test/resources/com/example/log4j-ut.properties」
 *
 * 【注意事項】
 *  - __Databricksの場合、通常log4j.propertiesをカスタマイズしたい場合、下記サイトの推奨手順の通り、initスクリプトで、
 *  クラスタ上の既存のlog4j.propertiesに追記し更新する手順となっているため使用しないこと。__
 *    - 商用環境向けのプロファイルでは、通常「log4j.overwrite=false」とし無効化しておく
 *  - __ただし、Azure Monitorへのログ転送の場合は下記サイトの手順に従いDatabricksにinitスクリプトで「spark-listeners-loganalytics-_.jar」を
 * 配備し、本クラスを使用して、AzureMonitor用のAppenderに切り替わるように、log4j.propertiesを上書き適用する。__
 *    - この際、下記サイトの「com.microsoft.pnp.logging.Log4jConfiguration」クラスを使用しない。
 *    本システムのAPフレームワークは業務開発者が本クラスを適用せずとも各エントリポイントクラスが本クラスを利用して自動適用するようになっている。
 *
 * @see [[https://docs.microsoft.com/ja-jp/azure/databricks/kb/clusters/overwrite-log4j-logs]]
 * @see [[https://docs.microsoft.com/ja-jp/azure/architecture/databricks-monitoring/application-logs]]
 *
 *
 */
object Log4jConfiguration extends Logging {
  private val LOG4J_PROPERTIES_FILE_PREFIX = "log4j-"
  private val LOG4J_PROPERTIES_FILE_EXTENSION = ".properties"

  /**
   * application-XXX.propertiesでディレクトリ指定したlog4j-XXX.propertiesを適用する。(XXXはプロファイル名）
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
