package com.crm

import com.crm.dao.imp.ConfigDaoImpl
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.Breaks

object CRMAppStart {

 private val logger: Logger = LoggerFactory.getLogger(CRMAppStart.getClass)

  def main(args: Array[String]): Unit = {

    var data:Long=0L
    var pro_id:Long=0L
    if(args.length>1){
    pro_id=args(0).toLong
    data=args(1).toLong
    logger.info(s"参数信息为：日期（$data）:: 存储过程id（$pro_id）")
      var isHive = false
      var isES = false
    val list=ConfigDaoImpl.getConfigInfoList(pro_id,data)
    val iter = list.iterator()
     while (iter.hasNext){
        val temp = iter.next().DATA_SOURCE
        if("HIVE".equals(temp) && isHive){
          isHive=true
        }
        if("ES".equals(temp) && !isES){
          isES=true
          println("ESSOURCE")
        }
      }
//      if(isHive){
//        println("HIVESOURCE")
//      }
//      if(isES){
//        println("ESSOURCE")
//      }

  }

  }




}
