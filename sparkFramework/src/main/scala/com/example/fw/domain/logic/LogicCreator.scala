package com.example.fw.domain.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter

import scala.reflect.api.JavaUniverse
import scala.reflect.runtime.{universe => ru}

object LogicCreator {
  //参考
  //https://kazuhira-r.hatenablog.com/entry/20130121/1358780334
  //https://docs.scala-lang.org/ja/overviews/reflection/overview.html

  //引数あり版
  //Logicのコンストラクタに引数があれえばDataFileReaderWriterを渡す前提
  def newInstance[T](className: String, dataFileReaderWriter: DataFileReaderWriter[T]): Logic = {
    val constructorMirror: ru.MethodMirror = getConstructorMirror(className)
    val paramList = constructorMirror.symbol.asMethod.paramLists
    val logic = if (paramList.size == 0) {
      constructorMirror()
    } else if(paramList.size == 1) {
      constructorMirror(dataFileReaderWriter)
    } else {
      //TODO:引数0か1の前提になってしまっている
      ???
    }
    logic.asInstanceOf[Logic]
  }

  private def getConstructorMirror(className: String) = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.staticClass(className)
    val classMirror = mirror.reflectClass(classSymbol)
    val constructorSymbol = classSymbol.typeSignature.decl(ru.termNames.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructorSymbol)
    constructorMirror
  }

}
