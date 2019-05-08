import Dependencies._

name := "k-means-parallel"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= spark :+ scallop :+ typeSafeConfig

