package com.example.sample.common.receipt

import scala.reflect.runtime.universe._

trait ReceiptRecord {
  val dataShikibetsu: String
  val gyoNo: String
  val receEdaNo: String
  val recordType: String
}