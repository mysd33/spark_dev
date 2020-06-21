package com.example.sample.common.rule

import com.example.sample.common.entity.Person
import org.scalatest.funsuite.AnyFunSuite

/**
 * ビジネスルール（Rule）クラスの単体テストの例
 */
class PersonRuleTest extends AnyFunSuite {
  //テスト対象クラス
  val sut = new PersonRule

  test("PersonRule calAgeForStr") {
    assert(sut.calcAgeForStr("19980101") == 20)
  }

  test("PersonRule calAge") {
    assert(sut.calcAge(Person("taro", Option(20))) == 20)

  }

}

