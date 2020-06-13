package com.example.sample.common.udf

import com.example.sample.common.rule.PersonRule
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object PersonRuleUDF {
  def calcAge: UserDefinedFunction ={
    udf((s:String) => new PersonRule().calcAgeForStr(s))
  }
}
