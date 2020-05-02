package com.example.sample.common.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import com.example.fw.domain.logic.Logic
import com.example.fw.domain.model.DataFile
import com.example.sample.common.receipt.ReceiptRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

abstract class AbstractReceiptRDDBLogic(val dataFileReaderWriter: DataFileReaderWriter) extends Logic {
  val inputFile: DataFile[String]

  final override def execute(sparkSession: SparkSession): Unit = {
    val receipts = input(sparkSession)
    val result = process(receipts, sparkSession)
    output(result, sparkSession)
  }

  protected def input(sparkSession: SparkSession): RDD[String] = {
    //レセプト単位の要素とするRDDを返却
    dataFileReaderWriter.readToRDD(inputFile, sparkSession)
  }

  protected def process(receipts: RDD[String], sparkSession: SparkSession): RDD[(String, ReceiptRecord)]

  protected def output(result: RDD[(String, ReceiptRecord)], sparkSession: SparkSession): Unit
}
