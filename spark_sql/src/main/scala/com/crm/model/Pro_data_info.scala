package com.crm.model
import scala.collection.Map
case class Pro_data_info(id:Int,SQL:String,SQL_TYPE:Int,TABLE_NAME:String,DATA_SOURCE:String,CFG:Map[String,String])