package com.crm.service.Writer

import com.crm.model.Pro_data_info
import com.crm.service.Data_excute
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._

class ES_Wtiter(ssc: SparkSession,pro_data_info: Pro_data_info,np:Int) extends Data_excute(ssc: SparkSession,pro_data_info: Pro_data_info,np:Int){

  override def excute(sparkSession: SparkSession, pro_data_info: Pro_data_info, numsPartition: Int): Unit ={

    val sql = pro_data_info.SQL
    val cfg = pro_data_info.CFG
    val resource = pro_data_info.TABLE_NAME
    val df = sparkSession.sql(sql)
    if(cfg.isEmpty){
      df.saveToEs(resource)
    }else{
      df.saveToEs(resource,cfg)
    }
  }


}
