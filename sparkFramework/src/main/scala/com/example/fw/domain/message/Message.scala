package com.example.fw.domain.message

import java.text.MessageFormat
import java.util.{Locale, ResourceBundle}

/**
 * メッセージ管理機能を提供するオブジェクト
 * message.propertiesに定義したメッセージを取得する
 * message.propertiesになければ、AP基盤ライブラリ内のfw-message.propertiesに定義したメッセージを取得する
 * i.xxx.001={0}は情報メッセージです
 * w.xxx.001={0}は警告メッセージです
 * e.xxx.001={0}はエラーメッセージです
 */
object Message {
  private lazy val messageResourceBundle = ResourceBundle.getBundle("message", new ResourceBundleWithEncording())
  private lazy val fwMessageResourceBundle = ResourceBundle.getBundle("fw-message", new ResourceBundleWithEncording())
  private val EMPTY_STRING = ""

  /**
   * メッセージを取得する
   * @param key メッセージキー
   * @param args プレースホルダの文字列
   * @return メッセージ
   */
  def get(key: String, args: String*): String = {
    if (messageResourceBundle.containsKey(key)) {
      val messageTemplate = messageResourceBundle.getString(key)
      MessageFormat.format(messageTemplate, args:_*)
    } else if (fwMessageResourceBundle.containsKey(key)) {
      val messageTemplate = fwMessageResourceBundle.getString(key)
      MessageFormat.format(messageTemplate, args:_*)
    } else {
      EMPTY_STRING
    }
  }

}
