package com.example.sample.common.entity


/**
 * Personファイルを読み込むEntityクラスの例
 *
 * 別のcaseクラスを扱う例として、簡単のためPersonクラスをコピーして作成
 * @param name 名前
 * @param age 年齢
 */
case class PersonOther(name: String, age: Option[Long])
