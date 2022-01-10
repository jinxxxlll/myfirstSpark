package com.crm.dao

import com.crm.model.Pro_data_info

trait ConfigDao {
  def getConfigInfoList:List[Pro_data_info]
}
