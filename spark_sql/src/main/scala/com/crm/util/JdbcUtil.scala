package com.crm.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.slf4j.{Logger, LoggerFactory}

object JdbcUtil {
  private val logger: Logger = LoggerFactory.getLogger(JdbcUtil.getClass)
  var con:Connection=_
  def getConnection(driver:String,url:String,user:String,password:String):Connection= {
    Class.forName(driver)
    try {
      con=DriverManager.getConnection(url,user,password)
    }catch {
      case e:Exception=>{logger.error("获取连接失败！");e.printStackTrace()}
    }
    con
  }

  def release(con:Connection,stat:Statement,res:ResultSet)={

    if(con!=null){
      con.close()
    }
    if(stat!=null){
      stat.close()
    }
    if(res!=null){
      res.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val url = Config.Instance.getProperty("datasource.url")
    val user = Config.Instance.getProperty("datasource.username")
    val password = Config.Instance.getProperty("datasource.password")
    val driver = Config.Instance.getProperty("datasource.driver-class-name")

    getConnection(driver,url,user,password)
  }
}
