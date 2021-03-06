package com.crm.service.read

import com.crm.model.Pro_data_info
import com.crm.service.Data_excute
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

class ORC_Reader(ss: SparkSession,pro_data_info: Pro_data_info,np:Int) extends Data_excute(ss,pro_data_info,np){

  override def excute(sparkSession: SparkSession, pro_data_info: Pro_data_info, numsPartition: Int): Unit = {

    val tablename = pro_data_info.TABLE_NAME
    val path = pro_data_info.CFG("orcPath")
    if(path.isEmpty){
      throw new IllegalArgumentException("ORC参数有误！")
    }
    val df = sparkSession.read.orc(path)
    df.createTempView(tablename)
}




}
