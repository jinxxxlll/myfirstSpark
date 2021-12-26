package com.jinx

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
    val userinfo = sc.textFile(path + "user.log")


  }

  def main(args: Array[String]): Unit = {
  }
}