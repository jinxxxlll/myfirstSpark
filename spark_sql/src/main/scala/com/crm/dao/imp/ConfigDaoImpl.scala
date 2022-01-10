package com.crm.dao.imp

import java.sql.{CallableStatement, ResultSet}
import java.util._

import com.crm.dao.ConfigDao
import com.crm.model.Pro_data_info
import com.crm.util.{Config, JdbcUtil}
import org.slf4j.{Logger, LoggerFactory}

object ConfigDaoImpl extends ConfigDao{

  private val logger: Logger = LoggerFactory.getLogger(ConfigDaoImpl.getClass)
  override def getConfigInfoList(pro_id:Long,data:Long): List[Pro_data_info] = {
    val url = Config.getProperty("datasource.url")
    val user = Config.getProperty("datasource.username")
    val password = Config.getProperty("datasource.password")
    val driver = Config.getProperty("datasource.driver-class-name")
    val connection = JdbcUtil.getConnection(driver,url,user,password)
    val list:List[Pro_data_info] = new ArrayList[Pro_data_info]()
    var cs: CallableStatement = null
    val res:ResultSet=null

    try{
      cs=connection.prepareCall("call pro_data_main")
      cs.registerOutParameter(1,2)
      cs.registerOutParameter(2,2)
      cs.registerOutParameter(3,)


    }

  }



}
