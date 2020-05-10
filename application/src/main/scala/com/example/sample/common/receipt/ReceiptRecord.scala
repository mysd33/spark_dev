package com.example.sample.common.receipt

/**
 * レセプトレコードクラスを表すトレイト
 */
trait ReceiptRecord {
  val dataShikibetsu: String
  val gyoNo: String
  val receEdaNo: String
  val recordType: String
}