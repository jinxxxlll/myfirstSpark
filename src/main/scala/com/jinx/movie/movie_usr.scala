package com.jinx.movie

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object movie_usr {

  /**
   * usr: usrId + FM + age + occ + post
   * movie: movieId + name + type
   * occ: id + name
   * rating: usrId + movieId +ration + time
   *
   */
  def main(args: Array[String]): Unit = {
    //movie_usrInfo("G:\\Program Files (x86)\\spark_v1\\log\\")
    movie_usrInfo("D:\\Gdisk\\spark\\spark_v1\\log\\")
  }
  def movie_usrInfo(path:String): Unit ={
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("movie_usrInfo"))
    val movie = sc.textFile(path+"movie.log")
    val occ=sc.textFile(path+"occupation.log")
    val rating=sc.textFile(path+"ratings.log")
    val usrInfo= sc.textFile(path+"user.log").map(_.split("::")).map{ x=>
      (
        x(0),(x(1),x(2),x(3))
      )}
    val occ_name = trunsfrom(occ)
    val movid_name = trunsfrom(movie)
    val rating_name = trunsfrom(rating)
//    val res = rating_name.join(usrInfo).map(x=>{
//      (x._2._1,x._1,x._2._2)
//    })

    val res = usrInfo.map(x=>{(x._2._3,(x._1,x._2._1,x._2._2))}).join(occ_name).map(x=>(x._2._2,1)).reduceByKey(_+_)
    var num=0
    //res.foreach(x=>{num+=x._2; println(x);println(num)})
      var temp=0
     rating_name.map(x=>(x._1,1)).reduceByKey(_+_).foreach(x=>{if(x._2>temp)temp=x._2;println(temp)})

  }




  def trunsfrom(RDD: RDD[String]):RDD[(String,String)]={
    RDD.map(_.split("::")).map(x=>{(x(0),x(1))})
  }
}
