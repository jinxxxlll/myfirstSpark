package com.crm.service.read

import com.crm.model.Pro_data_info
import com.crm.service.{DataFactory, Data_excute}
import org.apache.spark.sql.SparkSession

class Hive_reader(ss: SparkSession,pro_data_info: Pro_data_info,np:Int) extends Data_excute(ss,pro_data_info,np){

  override def excute(sparkSession: SparkSession, pro_data_info: Pro_data_info, numsPartition: Int): Unit = {

  }




}
