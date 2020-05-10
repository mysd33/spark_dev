package com.example.sample.common.tokutei

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.xml.Elem

/**
 * 特定検診XMLのマッピングクラス
 */
object TokuteiKenshinMapper {
  def mapToTokuteiKennshinTuples(xml: Elem): Seq[(String, TokuteiKenshin)] = {
    val code: TokuteiKenshin = CodeTokuteiKenshinMapper.map(xml)
    val patientRole: TokuteiKenshin = PatientRoleTokuteiKenshinMapper.map(xml)

    //TODO:特定検診のタグごとに、文字列配列をcaseクラスにマッピングする処理を追加していく
    (TokuteiKenshinConst.Code, code
    ) :: (TokuteiKenshinConst.PatientRole, patientRole
    ) :: Nil
  }

  def extractRDD[T <: TokuteiKenshin : ClassTag]
  (recordTypeName: String, rdd: RDD[(String, TokuteiKenshin)]): RDD[T] = {
    rdd.filter(t => t._1 == recordTypeName)
      .map(t => t._2.asInstanceOf[T])
  }
}
