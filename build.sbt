name := "spark-api-parent"

organization := "tech.sourced"

version := "0.0.1"

scalaVersion := "2.12.3"

lazy val `scala-core` = project

lazy val `spark-core` = project.dependsOn(`scala-core`)
