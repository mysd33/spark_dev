package com.example.fw.domain.logic

import com.example.fw.domain.dataaccess.DataFileReaderWriter

import scala.reflect.runtime.{universe => ru}

/**
 * リフレクションでLogicインスタンスを生成するクラス
 */
object LogicCreator {
  //参考
  //https://kazuhira-r.hatenablog.com/entry/20130121/1358780334
  //https://docs.scala-lang.org/ja/overviews/reflection/overview.html

  /**
   * 引数のクラス名のLogicクラスのインスタンスを生成する
   *
   * @param className            Logicクラスの完全修飾名
   * @param dataFileReaderWriter Logicクラスのコンストラクタ引数に渡すDataFileReaderWriter
   * @param methodArgs           Logicクラスのメソッド引数
   * @return 生成されたLogicインスタンス
   */
  def newInstance(className: String, dataFileReaderWriter: DataFileReaderWriter, methodArgs: Array[String]): Logic = {
    val constructorMirror: ru.MethodMirror = getConstructorMirror(className)
    val paramList = constructorMirror.symbol.asMethod.paramLists
    val logic = if (paramList.size == 0) {
      constructorMirror()
    } else if (paramList.size == 1) {
      constructorMirror(dataFileReaderWriter)
    } else if (paramList.size == 2) {
      constructorMirror(dataFileReaderWriter, methodArgs)
    }else {
      ???
    }
    logic.asInstanceOf[Logic]
  }

  /**
   * 対象クラスのコンストラクタメソッドのMethodMirrorを作成する
   *
   * @param className クラスの完全修飾名
   * @return コンストラクタメソッドのMethodMirror
   */
  private def getConstructorMirror(className: String): ru.MethodMirror = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.staticClass(className)
    val classMirror = mirror.reflectClass(classSymbol)
    val constructorSymbol = classSymbol.typeSignature.decl(ru.termNames.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructorSymbol)
    constructorMirror
  }

}
