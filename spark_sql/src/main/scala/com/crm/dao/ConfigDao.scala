package com.crm.dao

import java.util

import com.crm.model.Pro_data_info

trait ConfigDao {

  def getConfigInfoList(pro_id: Long, data: Long): util.List[Pro_data_info]
}
