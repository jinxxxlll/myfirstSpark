package com.jinx

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType,DataType}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * usr: usrId + FM + age + occ + post
 * movie: movieId + name + type
 * occ: id + name
 * rating: usrId + movieId +ration + time
 *
 */

object FirstSparkSQL {

  /*
  特定电影中观看男性和女性不同年龄分别有多少人
   */
  def getNumOfGender(session: SparkSession,path:String,movieId:String)={
    //创建user信息
    val userinfo = session.sparkContext.textFile(path+"user.log")
    val ratingsinfo = session.sparkContext.textFile(path+"ratings.log")
    val movieinfo = session.sparkContext.textFile(path+"movies.log")
    val usrStuct = StructType("usrId::gender::age::occ::post".split("::").
      map(x=>StructField(x,StringType,true)))
    val usrRow = userinfo.map(_.split("::")).map(x => {
      Row(x(0).trim,
          x(1).trim,
          x(2).trim,
          x(3).trim,
          x(4).trim)
    })
    val usrDF = session.createDataFrame(usrRow, usrStuct)

    //创建评分数据
    val ratingsStuct = StructType("usrId::movieId::ration::time".split("::").
      map(x => StructField(x, StringType, true)))
    val ratingRow = ratingsinfo.map(_.split("::")).map(x => {
      Row(x(0).trim, x(1).trim, x(2).trim, x(3).trim)
    })
    val ratingsDF = session.createDataFrame(ratingRow, ratingsStuct)

    //获取指定电影的人数，按照性别分组
    //全部电影 val res = ratingsDF.
    val res1 = ratingsDF.filter(s" movieId = $movieId").
      join(usrDF, "usrId").select("gender").groupBy("gender").count().show()
    //获取在评分数据中不同性别的年龄段个多少人
    val res2 = ratingsDF.
      join(usrDF, "usrId").select("gender","age").groupBy("gender","age").count().show()
    //采用globaltempview实现
    ratingsDF.createGlobalTempView("ratings")
    usrDF.createGlobalTempView("usr")

    session.sql("select gender,age,count(1) from global_temp.usr a join global_temp.ratings b on a.usrId=b.usrId group by gender,age order by count(1)").show()

  }

  def main(args: Array[String]): Unit = {
    val session = new SparkSession.Builder().
      appName("startSparkSQL").config(new SparkConf().setMaster("local")).getOrCreate()
    getNumOfGender(session,"D:\\Gdisk\\spark\\spark_v1\\log\\","6750")
  }
}