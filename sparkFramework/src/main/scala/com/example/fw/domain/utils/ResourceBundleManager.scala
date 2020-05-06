package com.example.fw.domain.utils

import java.util.ResourceBundle

/**
 * プロパティ管理機能を提供するオブジェクト
 *
 * Spark AP実行時に、javaVMオプションを「-Dacitive.profile=xxx」で指定するか
 * 環境変数「ACTIVE_PROFILE=xxx」を指定することで、(xxxはプロファイル名)
 * 「application-xxx.properties」(xxxはプロファイル名）に記載のプロパティを適用する。
 * この機能を利用して、開発端末、単体テスト、結合テスト、商用といった
 * 動作環境によってプロパティファイルを分けて定義することで
 * ファイルパス等の環境依存のパラメータを切替えることができる。
 *
 * なお「application.properties」はプロファイル名によらない動作環境によって
 * 変化しない共通のプロパティを定義する。
 */
object ResourceBundleManager {
  //propertieファイル名の定義
  private val PROPERTIES_FILE_NAME = "application"
  private val SEPARATOR = "-"
  //-Dオプションでのプロファイル指定のキー名
  private val ACTIVE_PROFILE_KEY = "active.profile"
  //環境変数でのプロファイル指定の環境変数名
  private val ACTIVE_PROFILE_ENV = "ACTIVE_PROFILE"
  //デフォルトのプロファイル名はprod
  private val DEFAULT_PROFILE = "prod"
  private lazy val rb = ResourceBundle.getBundle(PROPERTIES_FILE_NAME)
  private lazy val profile = {
    val envProfile = System.getenv(ACTIVE_PROFILE_ENV)
    val sysPropProfile = System.getProperty(ACTIVE_PROFILE_KEY)
    if (envProfile != null && !envProfile.isEmpty) {
      envProfile
    } else if (sysPropProfile != null && !sysPropProfile.isEmpty) {
      sysPropProfile
    } else if (rb.containsKey(ACTIVE_PROFILE_KEY)) {
      rb.getString(ACTIVE_PROFILE_KEY)
    } else {
      DEFAULT_PROFILE
    }
  }

  private lazy val rbProfile = ResourceBundle.getBundle(PROPERTIES_FILE_NAME + SEPARATOR + profile)

  /**
   * 現在有効になっているプロファイル名を取得する
   *
   * @return 現在有効になっているプロファイル名
   */
  def getActiveProfile(): String = profile

  /**
   * 指定したキーに対して
   * 現在有効になっているプロファイルで管理されているプロパティの値を取得する
   *
   * @param key プロパティのキー名
   * @return プロパティの値
   */
  def get(key: String): String = {
    val tempValue = System.getProperty(key)
    val value = if (tempValue != null && !tempValue.isEmpty) {
      tempValue
    } else if (rb.containsKey(key)) {
      rb.getString(key)
    } else if (rbProfile.containsKey(key)) {
      rbProfile.getString(key)
    } else {
      null
    }
    value
  }
}
