package com.crm.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

object JdbcUtil {

  def getConnection(driver:String,url:String,user:String,password:String):Connection= {
      Class.forName(driver)
      val con:Connection =DriverManager.getConnection(url,user,password)
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

}
