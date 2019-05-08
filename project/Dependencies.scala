import sbt._

object Dependencies {
  object versions {
    val scala = "2.12.8"
    val spark = "2.4.3"
    val scallop = "3.2.0"
    val typeSafeConfig = "1.3.4"
  }

  val sparkSql = "org.apache.spark" %% "spark-sql" % versions.spark
  val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark
  val scallop = "org.rogach" %% "scallop" % versions.scallop
  val typeSafeConfig = "com.typesafe" % "config" % versions.typeSafeConfig
  
  val spark: Seq[ModuleID] = Seq(sparkSql, sparkCore)
}
