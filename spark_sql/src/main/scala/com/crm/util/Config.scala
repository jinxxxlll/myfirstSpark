package com.crm.util

import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

import scala.util.Properties

object Config {


  private val logger: Logger = LoggerFactory.getLogger(Config.getClass)
  private var properties: Properties = _

  def loadProperties(source: String):Properties ={
    val properties = new Properties
    var stream = Config.getClass.getResourceAsStream(source)
      if(stream==null){
        val loader = Thread.currentThread().getContextClassLoader
        stream=loader.getResourceAsStream(source)
      }
      if(stream!=null){
        properties.load(stream)
        stream.close()
      }
      properties
      }

  def getProperty(value:String):String={
    if(properties==null){
      properties=getProperties()
    }
    properties.getProperty(value)

  }

  def getProperties(): Properties={
    if(properties==null){
      loadProperties("config.properties")
    }else{
      properties
    }

  }

}
