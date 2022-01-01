package com.jinx.demo

import java.util

import scala.collection.mutable

object Solution {
  /**
   * 代码中的类名、方法名、参数名已经指定，请勿修改，直接返回方法规定的值即可
   *
   * @param s string字符串
   * @param n int整型
   * @return string字符串
   */
  def trans(s: String,n: Int): String = {
    // write code here
    var left=false
    var right=false
    var temp=""
    var res=""
    if(s(0).toString.equals(" ")){
      left=true
    }else if(s(s.length-1).toString.equals(" ")){
      right=true
    }
    s.split(" ").reverse.foreach(x=>x.foreach(y=>
    if(y.isLower){
      res+=y.toUpper
    }else if(y.isUpper){
      res+=y.toLower
    }else{
    }
    )
    )
    res=res.trim
    if(left){
      res=res+" "
    }else if(right){
      res=" "+res
    }
    res
  }

  def main(args: Array[String]): Unit = {
    println(trans("h i ", 1))
  }
}
