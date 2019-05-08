package com.kmeans.utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

case class ApplicationProperties(cliObject: CliObject) {
  private val config: Config = ConfigFactory.parseFile(new File(cliObject.configPath))

  val kParam = config.getString("params.k")
  val sourceDataPath = config.getString("source-data.path")
}
