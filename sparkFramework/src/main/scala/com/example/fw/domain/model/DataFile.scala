package com.example.fw.domain.model

trait DataFile[T] {
  //TODO: ベースパスの置き換えができるようにする
  val filePath: String
}
