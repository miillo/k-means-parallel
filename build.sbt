import Dependencies._

name := "k-means-parallel"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= spark ++ vegas :+ scallop :+ typeSafeConfig :+ scalaMeter