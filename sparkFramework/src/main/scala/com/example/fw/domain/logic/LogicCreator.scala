package com.example.fw.domain.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter
import scala.reflect.runtime.{universe => ru}

object LogicCreator {
  def newInstance[T](classname: String, dataFileReaderWriter: DataFileReaderWriter[T]): Logic = {
    //参考
    //https://kazuhira-r.hatenablog.com/entry/20130121/1358780334
    //https://docs.scala-lang.org/ja/overviews/reflection/overview.html
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.staticClass(classname)
    val classMirror = mirror.reflectClass(classSymbol)
    val constructorSymbol = classSymbol.typeSignature.decl(ru.termNames.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructorSymbol)
    //Logicのコンストラクタは1つ引数としてDataFileReaderWriterを渡す前提
    //TODO:本当は引数1つのコンストラクタがあることをチェックすると良い
    val logic = constructorMirror(dataFileReaderWriter)
    logic.asInstanceOf[Logic]
  }

}
