package com.example.sample.common.tokutei

import scala.xml.Elem

/**
 * 特定検診クラスのPatientRoleタグを扱う例。
 *
 * エンティティクラス同様caseクラスで作成。
 *
 */
case class PatientRole(hokenjaNo: String,
                       hihokenshaShoKigo: String,
                       hihokenshaShoNo: String) extends TokuteiKenshin

/**
 * XMLのマッピング定義
 */
object PatientRoleTokuteiKenshinMapper {
  def map(xml: Elem): PatientRole = {
    val patientRoleTag = xml \ "recordTarget" \ "patientRole"
    val idTags = patientRoleTag \\ "id"
    val hokenjaNo = idTags(0) \@ "extension"
    val hihokensaKigo = idTags(1) \@ "extension"
    val hihokensaNo = idTags(2) \@ "extension"
    PatientRole(hokenjaNo, hihokensaKigo, hihokensaNo)
  }
}