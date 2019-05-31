package com.kmeans.utils

import org.rogach.scallop.{ScallopConf, ScallopOption}

final case class CliObject(configPath: String)

/**
  * Class responsible for reading command line arguments
  *
  * @param args arrays with command line arguments
  */
class CliReader(args: Array[String]) extends ScallopConf(args) {
  private val configPath: ScallopOption[String] = opt[String](name = "config", required = true)

  /**
    * Creates object with parsed command line information
    *
    * @return cli object
    */
  def createCliObject(): CliObject = CliObject(configPath.apply())
  verify()
}
