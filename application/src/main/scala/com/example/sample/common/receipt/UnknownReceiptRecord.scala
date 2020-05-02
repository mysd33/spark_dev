package com.example.sample.common.receipt

case class UnknownReceiptRecord(items: Array[String]) extends ReceiptRecord {
  private val EMPTY_STRING = ""
  override val dataShikibetsu = if (items.length > 0) items(0) else EMPTY_STRING
  override val gyoNo = if (items.length > 1) items(1) else EMPTY_STRING
  override val receEdaNo = if (items.length > 2) items(2) else EMPTY_STRING
  override val recordType = if (items.length > 3) items(3) else EMPTY_STRING
}
