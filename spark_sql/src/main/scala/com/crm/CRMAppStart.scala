package com.crm

import java.util

import com.crm.dao.imp.ConfigDaoImpl
import com.crm.model.Pro_data_info
import com.crm.service.DataFactory
import com.crm.util.Config
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Breaks

object CRMAppStart {

  private val logger: Logger = LoggerFactory.getLogger(CRMAppStart.getClass)

  def main(args: Array[String]): Unit = {

    var data:Long=0L
    var pro_id:Long=0L
    if(args.length==2){
      pro_id=args(0).toLong
      data=args(1).toLong
      logger.info(s"参数信息为：日期（$data）:: 存储过程id（$pro_id）")
      val builder = SparkSession.builder
      var isHive = false
      var isES = false
      val list=ConfigDaoImpl.getConfigInfoList(pro_id,data)
      var iter = list.iterator()
      while (iter.hasNext){
        val temp = iter.next().DATA_SOURCE
        if("HIVE".equals(temp) && isHive){
          isHive=true
        }
        if("ES".equals(temp) && !isES){
          isES=true
        }
      }
      if(isHive){
        logger.info("配置HIVE参数中...")
        val dir = Config.Instance.getProperty("spark.sql.warehouse.dir")
        val uris = Config.Instance.getProperty("hive.metastore.uris")
        builder.config("spark.sql.warehouse.dir",dir)
        builder.config("hive.metastore.uris",uris)
        builder.enableHiveSupport()
      }
      if(isES){
        val nodes = Config.Instance.getProperty("es.nodes")
        val user = Config.Instance.getProperty("es.net.http.auth.user")
        val password = Config.Instance.getProperty("es.net.http.auth.pass")
        builder.config("es.nodes",nodes)
        builder.config("es.net.http.auth.user",user)
        builder.config("es.net.http.auth.pass",password)
      }
      builder.config(new SparkConf().setMaster("local"))
      val ssc = builder.getOrCreate()
      val np = Config.Instance.getProperty("numPartitions").toInt
      iter=list.iterator()
      while(iter.hasNext){
        val temp = iter.next()
        val data_excute = DataFactory.CreatDataClass(ssc,temp,np)
        if(data_excute!=null){
          data_excute.excute(ssc,temp,np)
        }else{
          logger.error("对应处理类不存在！请检查！")
        }
      }

    }
  }




}
