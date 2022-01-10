package com.jinx

import java.util

import com.jinx.model.{Ratings, User}
import org.apache.spark
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object AnalysisUsr {
  //map,flatmap,partitionmap区别
  def demo(session:SparkSession,path:String)={
    val usrStruct = StructType("usrId::FM::age::occ::post".split("::").map(x => StructField(x, StringType, true)))
    val usrinfo = session.sparkContext.textFile(path + "user.log").map(_.split("::"))
    val usrROW = usrinfo.map(x => {
      Row(x(0), x(1), x(2), x(3), x(4))
    })
    val ratinginfo = session.sparkContext.textFile(path + "ratings.log")
    val ratingsStuct = StructType("usrId::movieId::ration::time".split("::").map(x => StructField(x, IntegerType, true)))
    val ratingRDD = ratinginfo.map(_.split("::")).map(x => {
      Row(x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt)
    })

    import session.implicits._
    val ratingDS = session.createDataFrame(ratingRDD,ratingsStuct).as[Ratings]
    val usrDF = session.createDataFrame(usrROW,usrStruct)
    val usrDS = usrDF.as[User]
    val res = usrDS.map(x=>User2(x.usrId,x.age.toInt))
    //
    res.flatMap(x => x match {
      case User2(usrId, age) if (usrId == "90090206") => List((usrId, age + 100))
      case User2(usrId, age) => Array((usrId, age))
    }).foreach(x=>{
      if(x._1.equals("90090206"))
      println(x._1+"::"+x._2)
    })
    //按分区执行返回迭代器
    res.mapPartitions(x=>{
      var res=ArrayBuffer[(String,Int)]()
      while (x.hasNext){
        res+=((x.next().usrId,x.next().age+1000))
      }
      res.iterator
    }).show()
//
//    println(res.rdd.partitions.size)
    //返回元组
    usrDS.joinWith(ratingDS,usrDS.col("usrId")===ratingDS.col("usrId"))
  }


  case class User2(usrId: String, age: Int)

  def main(args: Array[String]): Unit = {
    val path:String="D:\\Gdisk\\spark\\spark_v1\\log\\"
    val session = new spark.sql.SparkSession.Builder().appName("demo").config(new SparkConf().setMaster("local")).getOrCreate()
    demo(session,path)
  }

}
