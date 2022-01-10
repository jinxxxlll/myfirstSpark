package com.crm

import org.slf4j.{Logger, LoggerFactory}

object CRMAppStart {

 private val logger: Logger = LoggerFactory.getLogger(CRMAppStart.getClass)

  def main(args: Array[String]): Unit = {

    var data:Long=null
    var pro_id:Long=null
  if(args.length>1){
    pro_id=args(0).toLong
    data=args(1).toLong
    logger.info(s"参数信息为：日期（$data）:: 过程id（$pro_id）")

  }

  }




}
