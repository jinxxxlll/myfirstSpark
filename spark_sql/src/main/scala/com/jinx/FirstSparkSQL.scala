package com.jinx

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * usr: usrId + FM + age + occ + post
 * movie: movieId + name + type
 * occ: id + name
 * rating: usrId + movieId +ration + time
 *
 */

object FirstSparkSQL {

  def getNumOfGender(path:String)={

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("getNumOfGender"))
    val session = new SparkSession.Builder().appName("getNumOfGender")
    val userinfo = sc.textFile(path + "user.log")
    val value = StructType("usrid::gender::age::occ::post".split("::").
      map(x=>StructField(x,StringType,true)))
    val usrRow = userinfo.map(_.split("::")).map(x => {
      Row(x(0).trim,
          x(1).trim,
          x(2).trim,
          x(3).trim,
          x(4).trim)
    })


  }

  def main(args: Array[String]): Unit = {
  }
}