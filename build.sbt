import Dependencies.{scalaTest, _}
import sbt.Keys.libraryDependencies

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "tech.spark.api",
      scalaVersion := "2.11.11",
      version := "0.1.0-SNAPSHOT"
    )),
    name := "spark-api",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += sparkSql % Compile
  )
