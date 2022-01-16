package com.jinx

import com.crm.dao.imp.ConfigDaoImpl
import com.crm.model.Pro_data_info
import java.util._

import com.crm.CRMAppStart
import org.junit.Test

class TestConf {

  @Test
  def testConfig():Unit={
    ConfigDaoImpl.getConfigInfoList(6,20211201)

  }

  @Test
  def main(): Unit = {
    CRMAppStart.main(Array("6","20211201"))
  }
}
