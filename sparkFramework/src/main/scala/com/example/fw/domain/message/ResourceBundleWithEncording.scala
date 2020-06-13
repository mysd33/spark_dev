package com.example.fw.domain.message

import java.io.{BufferedReader, InputStreamReader}
import java.util.{Locale, PropertyResourceBundle, ResourceBundle}
import java.util.ResourceBundle.Control

import com.example.fw.domain.utils.Using._

/**
 * プロパティファイルをUTF-8でエンコーディング指定して取得するのためのクラス
 */
class ResourceBundleWithEncording extends Control {
  private val SUFFIX = "properties"
  private val ENCODE = "UTF-8"

  /**
   * UTF-8でプロパティファイルを取得するよう、java.util.ResourceBundle.Control.newBundleメソッドをオーバライド
   */
  override def newBundle(baseName: String, locale: Locale, format: String, loader: ClassLoader, reload: Boolean): ResourceBundle = {
    val bundleName = toBundleName(baseName, locale)
    val resourceName = toResourceName(bundleName, SUFFIX)
    using(new BufferedReader(new InputStreamReader(loader.getResourceAsStream(resourceName), ENCODE))) {
      reader => {
        new PropertyResourceBundle(reader)
      }
    }
  }
}