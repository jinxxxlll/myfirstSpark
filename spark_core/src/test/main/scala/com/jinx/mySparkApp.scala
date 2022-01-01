package com.jinx

import java.{io, util}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.reflect.io.{File, Path}

object Other {
  var set:mutable.HashSet[String] =new mutable.HashSet[String]()
  def main(args: Array[String]): Unit = {

   out("max")


  }
  def out(s:String)={
    kelihua(s,(x,y)=>Math.max(x,y))

  }

  def wordcount(path:String)={
    val conf=new SparkConf()
    conf.setAppName("spark_v1")
    conf.setMaster("local")
    val sc=new SparkContext(conf)
    val Rdd=sc.textFile(path)
    val value = Rdd.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey(false)
    value.foreach(x=>println(x._2+":"+x._1))
    sc.stop()
  }

  def findadp(path:String): Unit ={

    val sc=new SparkContext(new SparkConf().setAppName("findadp").setMaster("local"))
    val Rdd=sc.textFile(path)
    Rdd.flatMap(x=>x.split(" ")).foreach(x=>
      if(x.contains(",")){
        x.split(",").foreach(y=>
          if(y.contains("ADP") && !y.contains("ADP_USR")){
            set.add(y)
          })
      }else if(x.contains("ADP") && !x.contains("ADP_USR")){
      set.add(x)
      })

    sc.stop()
  }

  def kelihua(s:String,value:(Int,Int)=>Int): Unit ={
    println(value)
  }


}
