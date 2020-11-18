package com.epam.util

import java.io.{FileNotFoundException, InputStream}
import java.util.Properties

import com.epam.util.Settings.Settings
import org.slf4j.{Logger, LoggerFactory}

object PropertyReader {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val path = "application.properties"
  private val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream(path)
  private val properties: Properties = new Properties()

  if (inputStream != null) {
    properties.load(inputStream)
  }
  else {
    logger.error(s"properties file cannot be loaded at path $path")
    throw new FileNotFoundException("Properties file cannot be loaded")
  }

  def getProperty(settingsKey: Settings): String = {
    properties.getProperty(settingsKey.toString)
  }
}
