package com.example.sample.common.rule

import com.example.sample.common.entity.Person


class PersonRule extends Serializable {
  //mapメソッド内でも実行可能なようSerializableを実装
  def calcAge(person: Person): Long = {
    //実際には生年月日から年齢計算するビジネスルールとかを想定
    person.age.getOrElse(0)
  }
}
