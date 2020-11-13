package com.epam

import java.io.FileNotFoundException
import java.net.URL
import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

object PropertyReader {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val path = "../../application.properties";
  private val url: URL = getClass.getResource(path)
  private val properties: Properties = new Properties()

  if (url != null) {
    val source = Source.fromURL(url)
    properties.load(source.bufferedReader())
  }
  else {
    logger.error(s"properties file cannot be loaded at path $path")
    throw new FileNotFoundException("Properties file cannot be loaded")
  }

  def getProperty(key: String): String = {
    properties.getProperty(key)
  }
}
