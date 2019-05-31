import sbt._

/**
  * Object with versions and libraries for project
  */
object Dependencies {

  object versions {
    val scala = "2.11.12"
    val spark = "2.3.3"
    val scallop = "3.2.0"
    val typeSafeConfig = "1.3.4"
    val scalaMeter = "0.17"
    val vegas = "0.3.11"
  }

  val sparkSql = "org.apache.spark" %% "spark-sql" % versions.spark
  val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark
  val scallop = "org.rogach" %% "scallop" % versions.scallop
  val typeSafeConfig = "com.typesafe" % "config" % versions.typeSafeConfig
  val scalaMeter = "com.storm-enroute" %% "scalameter-core" % versions.scalaMeter
  val vegasCore = "org.vegas-viz" %% "vegas" % versions.vegas
  val vegasSpark = "org.vegas-viz" %% "vegas-spark" % versions.vegas

  val spark: Seq[ModuleID] = Seq(sparkSql, sparkCore)
  val vegas: Seq[ModuleID] = Seq(vegasCore, vegasSpark)
}
