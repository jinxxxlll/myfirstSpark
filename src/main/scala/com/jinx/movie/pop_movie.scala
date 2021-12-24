package com.jinx.movie

import com.jinx.model.DoubleSort
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.HashSet
import scala.collection.parallel.immutable

/**
 * usr: usrId + FM + age + occ + post
 * movie: movieId + name + type
 * occ: id + name
 * rating: usrId + movieId +ration + time
 *
 */
object pop_movie {

  /*
  最受欢迎的电影（TOP10）
   */
  def movie_pop(path:String)={
    val sc = new SparkContext(new SparkConf().setAppName("popMovie").setMaster("local"))
    val movie_info = sc.textFile(path+"movies.log").map(_.split("::")).map(x=>{(x(0),(x(1),x(2)))})
    val rating_info = sc.textFile(path+"ratings.log").map(x=>x.split("::")).map{x=>
      (x(1),(x(2).toInt,1))
    }
    val res = rating_info.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>
      (x._2._1/x._2._2,x._1)
    ).sortByKey(false).take(10)
    val value = sc.makeRDD(res).map(x=>(x._2,x._1)).join(movie_info).map(x=>(x._2._1,(x._1,x._2._2))).sortByKey(false)
    value.foreach(println)
  }

  /*
  观看人数最多的电影
   */
  def movie_most(path:String)={
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("Most_movie"))
    val res=sc.textFile(path+"ratings.log").map(_.split("::"))
      .map(x=>(x(1),1))
      .reduceByKey(_+_)
      .map(x=>(x._2,x._1)).sortByKey(false).take(10)
    res.foreach(println)
   sc.stop()
  }

  /*
  各个类型TOP10电影
   */
  def type_movie(path:String)={
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("type_movie"))
    val rating = sc.textFile(path+"ratings.log").map(_.split("::")).map(x=>(x(1),x(2).toInt))
    val movie = sc.textFile(path+"movies.log").map(_.split("::")).map(x=>(x(0),(x(1),x(2))))
    val arr=Array("Action", "Adventure", "Animation", "Childdren", "Comedy", "Crime", "Documentary",
      "Drama", "Fantasy", "Film-Noir", "Horror", "Musical", "Mystety", "Romance", "Sci-Fi",
      "Thriller", "War", "Western")
    for (elem <- arr) {
      val res=rating.join(movie).map(x=>{
        ((x._2._2._2,x._1),(x._2._1,x._2._2._1))
      }).reduceByKey((x,y)=>(x._1+y._1,x._2))
        .map(x=>((x._1._1,x._2._1),(x._2._2,x._1._2))).filter(x=>x._1._1.equals(elem))
        .sortByKey(false).take(10)
      sc.makeRDD(res).saveAsTextFile(path+s"type\\$elem")
    }
    sc.stop()
  }

  /*
  各个年龄段TOP10电影
  MapJoin的使用
   */
  def age_movie(path:String)={
    val sc = new SparkContext(new SparkConf().setAppName("age_movie").setMaster("local"))
    val ages = Array(1, 18, 25, 30, 35, 40, 45, 50, 56)
    val movieinfo = sc.textFile(path+"movies.log").map(_.split("::")).map(x=>(x(0),(x(1),x(2))))
    val usrinfo = sc.textFile(path+"user.log").map(_.split("::")).map(x=>(x(0),x(2)))
    val ratings = sc.textFile(path+"ratings.log").map(_.split("::")).map(x=>(x(0),x(1),x(2)))
    for (elem <- ages) {
      val temp =usrinfo.filter(x=>x._2.toInt==elem)
      val value = HashSet()++temp.map(_._1).collect()
      val broadcast = sc.broadcast(value)
      val res=ratings.filter(x=>broadcast.value.contains(x._1)).map(x=>(x._2,(x._3.toDouble,1)))
        .reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).map(x=>(x._2._1/x._2._2,x._1))
        .sortByKey(false).take(10)
      val result = sc.makeRDD(res).map(x=>(x._2,x._1)).join(movieinfo)
      result.saveAsTextFile(path+s"ages\\$elem")
    }
    sc.stop()
  }

/*
/ 优先时间排序，然后usr_id 排序
 */
  def sort(path:String)={
    val sc=new SparkContext(new SparkConf().setAppName("sort").setMaster("local"))
    val value = sc.textFile(path+"ratings.log").map(x=>{
      val strings = x.split("::")
      (new DoubleSort(strings(3).toDouble,strings(0)toInt),x)})
    value.sortByKey(false).take(10).foreach(println)
  }

  def main(args: Array[String]): Unit = {
    //movie_most("G:\\Program Files (x86)\\spark_v1\\log\\")
    //type_movie("G:\\Program Files (x86)\\spark_v1\\log\\")
    //sort(,"D:\\Gdisk\\spark\\spark_v1\\log\\")
  }
}
