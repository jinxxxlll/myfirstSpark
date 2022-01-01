package com.jinx

import com.jinx.model.{Ratings, User}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * usr: usrId + FM + age + occ + post
 * movie: movieId + name + type
 * occ: id + name
 * rating: usrId + movieId +ration + time
 *
 */
object SparkSQLDS {

  var usrDF: DataFrame = _
  var movieDF:DataFrame=_
  var occDF:DataFrame=_
  var ratingsDF:DataFrame=_
  //优化代码块，减少冗余
  def inittialDF(session: SparkSession,path:String): Unit ={
    //构建DF
    val userStruct = StructType("usrId::FM::age::occ::post".split("::").map(x => StructField(x, StringType, true)))
    val usrinfo = session.sparkContext.textFile(path + "user.log").map(_.split("::"))
    val usrROW = usrinfo.map(x => {
      Row(x(0), x(1), x(2), x(3), x(4))
    })
    val ratinginfo = session.sparkContext.textFile(path + "ratings.log")
    val ratingsStuct = StructType("usrId::movieId::ration::time".split("::").map(x => StructField(x, IntegerType, true)))
    val ratingRDD = ratinginfo.map(_.split("::")).map(x => {
      Row(x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt)
    })
    val movieRDD = session.sparkContext.textFile(path + "movies.log").map(_.split("::")).map(x=>{
      Row(x(0),x(1),x(2))
    })
    val movieStuct = StructType("movieId-name-type".split("-").map(x => StructField(x, StringType, true)))

    val occROW = session.sparkContext.textFile(path + "occ.log").map(_.split("::")).map(x => {
      Row(x(0), x(1))
    })
    val occStruct = StructType("id::name".split("").map(x => {
      StructField(x, StringType, true)
    }))

     ratingsDF = session.createDataFrame(ratingRDD, ratingsStuct)
     usrDF = session.createDataFrame(usrROW, userStruct)
     movieDF=session.createDataFrame(movieRDD,movieStuct)
    occDF=session.createDataFrame(occROW,occStruct)
  }

  /*
  特定电影中观看者男性和女性不同年龄段的人数
   */
  def numofgender(session:SparkSession,path:String,movieId:String)={
    //隐式转换
    import session.implicits._
    //转换DS
    val raDS = ratingsDF.as[Ratings]
    val usrDS = usrDF.as[User]
    //过滤出电影
    val movie = raDS.filter(s"movieId=$movieId")
    //全部计算
//    raDS.join(usrDS,"usrId").groupBy("movieId","age","FM").count()
//      .select("movieId","age","FM","count").orderBy($"count".desc)
//      .show()
    //特定电影计算
    movie.join(usrDS,"usrId").groupBy("movieId","age","FM").count()
      .select("movieId","age","FM","count")
      .show()
  }

  /*
  所有电影中平均得分最高的电影
   */
  def avg_movie(session: SparkSession,path:String): Unit ={
    import session.implicits._
    val ratingDS = ratingsDF.as[Ratings]
    ratingDS.groupBy("movieId").avg("ration").orderBy($"avg(ration)".desc).show(10)
  }

  /*
  所有电影中观看人数最多的电影
   */
  def most_movie(session: SparkSession,path:String)={
    import session.implicits._
    val ratingDS = ratingsDF.as[Ratings]
    ratingDS.groupBy("movieId").count().orderBy($"count".desc).show(10)

  }

  /*
  所有电影中最受男女性喜爱的电影
   */
  def genderOfmovie(session: SparkSession): Unit ={
    import session.implicits._
    val ratingDS = ratingsDF.as[Ratings]
    val userDS = usrDF.as[User]
    //男性
    userDS.filter("FM='F'").join(ratingDS,"usrId")
      .groupBy("movieId").avg("ration").orderBy($"avg(ration)".desc).show(10)
  }

  /*
  核心用户最喜爱的电影
   */
  def mostLikeMoive(session: SparkSession,age:String)={

    //如何计算核心用户
    //
    import session.implicits._
    val userDS = usrDF.as[User]
    val ratingsDS = ratingsDF.as[Ratings]
    userDS.filter(s"age=$age").join(ratingsDS,"usrId")
      .groupBy("movieId").avg("ration").orderBy($"avg(ration)".desc)
      .show(10)
  }



  def main(args: Array[String]): Unit = {

    val session = new SparkSession.Builder().appName("FirstDS").config(new SparkConf().setMaster("local")).getOrCreate()
    val path:String="D:\\Gdisk\\spark\\spark_v1\\log\\"
    inittialDF(session,path)
    //numofgender(session,path,"1442")
    //avg_movie(session,path)
    //most_movie(session,path)
    //genderOfmovie(session)
    mostLikeMoive(session,"18")
  }

}
