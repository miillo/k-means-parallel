package com.kmeans.utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Class with application properties - read from configuration file
  *
  * @param cliObject command line data
  */
case class ApplicationProperties(cliObject: CliObject) {
  private val config: Config = ConfigFactory.parseFile(new File(cliObject.configPath))

  val iterations: Int = config.getInt("params.iterations")
  val kParam: Int = config.getInt("params.k")
  val sourceDataPath: String = config.getString("source-data.path")
}
