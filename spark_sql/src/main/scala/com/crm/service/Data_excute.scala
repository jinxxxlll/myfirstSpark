package com.crm.service


import com.crm.model.Pro_data_info
import com.crm.util.Config
import org.apache.spark.sql.SparkSession

abstract class Data_excute(ssc: SparkSession,pro_data_info: Pro_data_info,np:Int) {

  val sparkSession:SparkSession=ssc
  val pro_data:Pro_data_info=pro_data_info
  val numsPartition:Int=np
  def excute(sparkSession: SparkSession,pro_data_info: Pro_data_info,numsPartition:Int)

}
