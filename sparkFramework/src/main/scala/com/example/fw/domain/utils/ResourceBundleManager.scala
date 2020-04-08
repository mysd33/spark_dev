package com.example.fw.domain.utils

import java.util.ResourceBundle

object ResourceBundleManager {
  def get(key :String): String = {
    //TODO:プロパティがなかった時等の異常系の対処
    val rb = ResourceBundle.getBundle("application")
    val profile = rb.getString("active.profile")
    val rbProfile = ResourceBundle.getBundle("application-" + profile)
    rbProfile.getString(key)
  }
}
