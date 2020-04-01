package com.example.fw.domain.utils

object Using {
  //TODO: scala2.13.0からは標準でUsingクラスをサポート
  def using[A <: {def close()},B](resource:A)(func:A => B):Option[B] =
    try{
      Some( func(resource) ) //成功したら、Someに包んで返す
    }catch{
      case e:Exception => e.printStackTrace
        None //失敗したら、ログ吐いて、None返す
    }finally{
      if(resource != null) resource.close()
    }
}
