package com.example.fw.domain.utils

import java.util.ResourceBundle

object ResourceBundleManager {
  private val PROPERTIES_FILE_NAME = "application"
  private val ACTIVE_PROFILE_KEY = "active.profile"
  private val SEPARATOR = "-"

  def get(key :String): String = {
    val rb = ResourceBundle.getBundle(PROPERTIES_FILE_NAME)
    val profile = rb.getString(ACTIVE_PROFILE_KEY)
    val rbProfile = ResourceBundle.getBundle(PROPERTIES_FILE_NAME + SEPARATOR + profile)
    val value = if (rb.containsKey(key)) {
      rb.getString(key)
    } else if (rbProfile.containsKey(key)) {
      rbProfile.getString(key)
    } else {
      null
    }
    value
  }
}
