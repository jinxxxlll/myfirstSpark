package com.jinx.demo

object Test {
  def main(args: Array[String]): Unit = {

    val list=List(1,3,2,5,245,43,5)

    var x=12
    val res=(a:Int)=>a*a+x

    ++(1, 2)


  }

  def ++(a:Int*)={
    var res = 0
    a.foreach(res+=_)
    println(res)

  }


  def  f(a:Int,b:Int)={
       a+b
  }
}
