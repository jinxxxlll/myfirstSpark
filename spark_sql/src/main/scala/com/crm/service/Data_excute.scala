package com.crm.service


import com.crm.model.Pro_data_info
import com.crm.util.Config
import org.apache.spark.sql.SparkSession

abstract class Data_excute(ss: SparkSession,pro_data_info: Pro_data_info,np:Int) {

  var sparkSession:SparkSession=_
  var pro_data:Pro_data_info=_
  var numsPartition:Int=_
  def excute(sparkSession: SparkSession,pro_data_info: Pro_data_info,numsPartition:Int)


  def this(sparkSession: SparkSession, pro_data: Pro_data_info) {
    this(sparkSession,pro_data,Config.Instance.getProperty("numPartitions").toInt)
  }

}
