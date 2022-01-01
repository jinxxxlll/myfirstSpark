package com.jinx

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}
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
  /*
      所有电影中平均得分最高的电影
       */
  def pop_movie(session: SparkSession,path:String)={

    val ratinginfo = session.sparkContext.textFile(path + "ratings.log")
    val ratingsStuct = StructType("usrId::movieId::ration::time".split("::").map(x => StructField(x, StringType, true)))
    val ratingRDD = ratinginfo.map(_.split("::")).map(x => {
      Row(x(0), x(1), x(2), x(3))
    }
    )
    val ratingDF = session.createDataFrame(ratingRDD, ratingsStuct)
    ratingDF.createTempView("ratings")
    session.sql("select movieId,avg(ration) from ratings group by movieId order by avg(ration) desc").show(10)
  }

  /*
  所有电影中观看人数最多的电影
   */
  def max_num_movie(session: SparkSession,path:String)={

    val ratinginfo = session.sparkContext.textFile(path + "ratings.log")
    val ratingsStuct = StructType("usrId::movieId::ration::time".split("::").map(x => StructField(x, StringType, true)))
    val ratingRDD = ratinginfo.map(_.split("::")).map(x => {
      Row(x(0), x(1), x(2), x(3))
    }
    )
    val ratingDF = session.createDataFrame(ratingRDD, ratingsStuct)
    ratingDF.createTempView("ratings")
    session.sql("select movieId,count(1) from ratings group by movieId order by count(1) desc").show(10)
  }

  /*
  最受男性和女性喜爱的电影TOP10
   */
  def get_movie(session:SparkSession,path:String)={
    val usrinfo = session.sparkContext.textFile(path + "user.log")
    val ratinginfo = session.sparkContext.textFile(path + "ratings.log")
    val ratingsStuct = StructType("usrId::movieId::ration::time".split("::").map(x => StructField(x, IntegerType, true)))
    val ratingRDD = ratinginfo.map(_.split("::")).map(x => {
      Row(x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt)
    }
    )
    val usrStuct = StructType("usrId::FM::age::occ::post".split("::").map(x => StructField(x, StringType, true)))
    val usrRow = usrinfo.map(_.split("::")).map(x => {
      Row(x(0), x(1), x(2), x(3), x(4))
    })
  //构建DF
    val ratingDF = session.createDataFrame(ratingRDD, ratingsStuct)
    val usrDF = session.createDataFrame(usrRow, usrStuct)
    //提前过滤
    val Fusr = usrDF.filter("FM= 'F' ")
    val Musr = usrDF.filter("FM= 'M' ")
    //计算结果
    val df = ratingDF.join(Fusr, "usrId").select("ration", "movieId").cache()
    val res = df.groupBy("movieId").avg("ration")
    res.sort(res.col("avg(ration)").desc).show(10)
  }

  /*
  所有电影中特定年龄群最喜爱电影
   */
  def centry_usr(session: SparkSession,path:String,age:String)={

    val ratingsRDD = session.sparkContext.textFile(path + "ratings.log").map(_.split("::")).map(x=>{
      Row(x(0).toInt,x(1).toInt,x(2).toInt,x(3).toInt)
    })
    val usrRDD = session.sparkContext.textFile(path + "user.log").map(_.split("::")).map(x=>{
      Row(x(0),x(1),x(2),x(3),x(4))
    })
    val movieRDD = session.sparkContext.textFile(path + "movies.log").map(_.split("::")).map(x=>{
      Row(x(0),x(1),x(2))
    })

    val usrStuct = StructType("usrId-FM-age-occ-post".split("-").map(x => StructField(x, StringType, true)))
    val ratingStuct = StructType("usrId::movieId::ration::time".split("::").map(x => StructField(x, IntegerType, true)))
    val movieStuct = StructType("movieId-name-type".split("-").map(x => StructField(x, StringType, true)))
    val movieDF = session.createDataFrame(movieRDD, movieStuct)

    val usrDF = session.createDataFrame(usrRDD, usrStuct)
    val ratingDF = session.createDataFrame(ratingsRDD, ratingStuct)
    //先过滤，再关联
    val res = usrDF.filter(s"age=$age").join(ratingDF, "usrId")
      .select("movieId","ration")
      .groupBy("movieId").avg("ration").
      join(movieDF,"movieId").
      select("name","type","avg(ration)")
    res.orderBy(res.col("avg(ration)").desc).show(10)
  }


  def main(args: Array[String]): Unit = {
    val path:String="D:\\Gdisk\\spark\\spark_v1\\log\\"
    val session = new SparkSession.Builder().
      appName("startSparkSQL").config(new SparkConf().setMaster("local")).getOrCreate()
      //getNumOfGender(session,"D:\\Gdisk\\spark\\spark_v1\\log\\","6750")
    //pop_movie(session,path)
    //max_num_movie(session, path)
    //get_movie(session,path)
    //centry_usr(session,path,"18")

  }
}