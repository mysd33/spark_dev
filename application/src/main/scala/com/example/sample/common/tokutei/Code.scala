package com.example.sample.common.tokutei
import scala.xml.Elem

/**
 * 特定検診クラスのCodeタグを扱う例。
 *
 * エンティティクラス同様caseクラスで作成。
 *
 */
case class Code(code: Long, codeSystem: String) extends TokuteiKenshin

/**
 * XMLのマッピング定義
 */
object CodeTokuteiKenshinMapper {
  def map(xml: Elem): Code = {
    val codeTag = xml \ "code"
    val codeValue = codeTag \@ "code"
    val codeSystem = codeTag \@ "codeSystem"
    Code(codeValue.toLong, codeSystem)
  }
}
