package com.example.fw.domain.utils

import java.util.ResourceBundle

object ResourceBundleManager {
  private val PROPERTIES_FILE_NAME = "application"
  private val ACTIVE_PROFILE_KEY = "active.profile"
  private val ACTIVE_PROFILE_ENV = "ACTIVE_PROFILE"
  private val SEPARATOR = "-"
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

private lazy val rbProfile = ResourceBundle.getBundle (PROPERTIES_FILE_NAME + SEPARATOR + profile)

def getActiveProfile (): String = profile

def get (key: String): String = {
  val tempValue = System.getProperty (key)
  val value = if (tempValue != null && ! tempValue.isEmpty) {
  tempValue
} else if (rb.containsKey (key) ) {
  rb.getString (key)
} else if (rbProfile.containsKey (key) ) {
  rbProfile.getString (key)
} else {
  null
}
  value
}
}
