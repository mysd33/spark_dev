// Databricks notebook source
case class TestKey(id: Long, str: String)

// COMMAND ----------

val rdd = sc.parallelize(Array((TestKey(1L, "abd"), "dss"), (TestKey(2L, "ggs"), "dse"), (TestKey(1L, "abd"), "qrf")))
rdd.groupByKey().collect

// COMMAND ----------

package com.databricks.example

case class TestKey(id: Long, str: String)

// COMMAND ----------

import com.databricks.example

val rdd = sc.parallelize(Array(
  (example.TestKey(1L, "abd"), "dss"), (example.TestKey(2L, "ggs"), "dse"), (example.TestKey(1L, "abd"), "qrf")))
rdd.groupByKey().collect

// COMMAND ----------

package x.y.z

object Utils {
  val aNumber = 5 // works!
  def functionThatWillWork(a: Int): Int = a + 1
}

// COMMAND ----------

import x.y.z.Utils

Utils.functionThatWillWork(Utils.aNumber)

// COMMAND ----------

package x.y.zpackage

import org.apache.spark.SparkContext

case class IntArray(values: Array[Int])

class MyClass(sc: SparkContext) {
  def sparkSum(array: IntArray): Int = {
    sc.parallelize(array.values).reduce(_ + _)
  }
}

object MyClass {
  def sparkSum(sc: SparkContext, array: IntArray): Int = {
    sc.parallelize(array.values).reduce(_ + _)
  }
}

// COMMAND ----------

import x.y.zpackage._

val array = IntArray(Array(1, 2, 3, 4, 5))

val myClass = new MyClass(sc)
myClass.sparkSum(array)

// COMMAND ----------

