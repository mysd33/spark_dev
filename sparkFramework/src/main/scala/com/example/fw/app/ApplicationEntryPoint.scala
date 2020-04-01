package com.example.fw.app

import com.example.fw.domain.logic.Logic
import com.example.fw.domain.utils.Using.using
import org.apache.spark.sql.SparkSession
import scala.reflect.runtime.{universe => ru}

object ApplicationEntryPoint {
  def main(args: Array[String]): Unit = {
    assert(args.length > 0)
    //TODO: コンストラクタ引数をとれるようにする
    val appName = args(0)
    //TODO: localモードかどうかの切替え
    val master = "local[*]"
    //TODO: プロパティで切替え
    val logLevel = "INFO"
    //Sparkの実行
    using(SparkSession.builder()
      .master(master)
      .appName(appName)
      .getOrCreate()
    ) { spark =>
      val sc = spark.sparkContext
      sc.setLogLevel(logLevel)
      //Logicインスタンスの実行
      val logic = newInstance(args(0))
      logic.execute(spark)
    }
  }

  private def newInstance(classname: String): Logic = {
    //参考
    //https://kazuhira-r.hatenablog.com/entry/20130121/1358780334
    //https://docs.scala-lang.org/ja/overviews/reflection/overview.html
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.staticClass(classname)
    val classMirror = mirror.reflectClass(classSymbol)
    val constructorSymbol = classSymbol.typeSignature.decl(ru.termNames.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructorSymbol)
    val logic = constructorMirror()
    logic.asInstanceOf[Logic]
  }
}
