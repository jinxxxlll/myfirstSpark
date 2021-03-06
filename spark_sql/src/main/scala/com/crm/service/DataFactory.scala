package com.crm.service

import java.util

import com.crm.model.Pro_data_info
import com.crm.service.Writer.ES_Wtiter
import com.crm.service.read.ORC_Reader
import org.apache.spark.sql.SparkSession

object DataFactory {
  val map=new util.HashMap[String,Class[_]]()

  def CreatDataClass(ssc:SparkSession,pro_data_info:Pro_data_info,numsparotition:Int):Data_excute={
    val temp = pro_data_info.SQL_TYPE+"::"+pro_data_info.DATA_SOURCE
    val excuteClass = map.get(temp)
    if(excuteClass!=null){
      val con = excuteClass.getConstructor(classOf[SparkSession],classOf[Pro_data_info],classOf[Int])
      val instance = con.newInstance(ssc,pro_data_info,numsparotition.asInstanceOf[Object])
      instance.asInstanceOf[Data_excute]
    }else{
      null
    }
  }

  def ControlDataType()={
    DataFactory.ControlDataType()
  }

object DataFactory{
  def ControlDataType():Unit={
    map.put("1::ORC",classOf[ORC_Reader])
    map.put("3::ES",classOf[ES_Wtiter])
    }
  }
}
