package com.crm.dao.imp

import java.sql.{CallableStatement, ResultSet}
import java.util

import com.crm.dao.ConfigDao
import com.crm.model.Pro_data_info
import com.crm.util.{Config, JdbcUtil}
import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{JavaConverters, Map, mutable}
object ConfigDaoImpl extends ConfigDao{

  private val logger: Logger = LoggerFactory.getLogger(ConfigDaoImpl.getClass)
  override def getConfigInfoList(pro_id:Long,date:Long): util.List[Pro_data_info] = {
    val url = Config.getProperty("datasource.url")
    val user = Config.getProperty("datasource.username")
    val password = Config.getProperty("datasource.password")
    val driver = Config.getProperty("datasource.driver-class-name")
    val connection = JdbcUtil.getConnection(driver,url,user,password)
    val list:util.List[Pro_data_info] = new util.ArrayList[Pro_data_info]()
    var cs: CallableStatement = null
    val res:ResultSet=null

    try{
      cs=connection.prepareCall("{call PRO_DATA_MAIN(?,?,?,?,?)}")
      cs.registerOutParameter(1,2)
      cs.registerOutParameter(2,12)
      cs.registerOutParameter(3, 2012)
      cs.setInt(4,pro_id.toInt)
      cs.setInt(5,date.toInt)
      cs.execute()
      val code = cs.getInt(1)
      val note = cs.getString(2)
      var dataClass:Pro_data_info =null
      if(code==1){
        val res=cs.getObject(3).asInstanceOf[ResultSet]
        while(res.next()){
          val id = res.getInt(1)
          val sql = res.getString(2)
          val sqlType = res.getInt(3)
          val tableName = res.getString(4)
          val sourceType = res.getString(5)
          val cfg = res.getString(6)
          if(StringUtils.isNotBlank(cfg)){
            val map= JavaConverters.mapAsScalaMap((new Gson).fromJson(cfg,classOf[util.HashMap[String,String]]))
            dataClass=Pro_data_info(id,sql,sqlType,tableName,sourceType,map.asInstanceOf[Map[String,String]])
          }else{
            dataClass=Pro_data_info(id,sql,sqlType,tableName,sourceType,null)
          }
          list.add(dataClass)
        }
      }else{
        logger.error(s"返回游标数据有误：$note")
      }
    } catch {
      case e:Exception => {logger.error(e.getMessage)}
    }finally {
      try{
        if(connection!=null){
          connection.close()
        }
        if(cs!=null){
          cs.close()
        }
        if(res!=null){
          res.close()
        }
      }catch {
        case e:Exception =>logger.error(e.getMessage)
      }
    }
    list
  }

}
