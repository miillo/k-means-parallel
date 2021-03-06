package com.kmeans

import com.kmeans.utils.{ApplicationProperties, CliReader}
import org.apache.spark.sql.SparkSession

/**
  * Main application class
  */
object Runner {
  def main(args: Array[String]): Unit = {
    val cliObject = new CliReader(args).createCliObject()
    val properties = new ApplicationProperties(cliObject)

    val sparkSession = SparkSession
      .builder()
      .appName("k-means-parallel")
      .master("local[10]")
      .getOrCreate()

    Executor.execute(sparkSession, properties)
  }
}
