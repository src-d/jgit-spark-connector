import Dependencies.{scalaTest, _}
import de.johoop.jacoco4sbt.XMLReport
import sbt.Keys.{libraryDependencies, resolvers}

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "tech.spark.api",
      scalaVersion := "2.11.11",
      version := "0.1.0-SNAPSHOT"
    )),
    name := "spark-api",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += sparkSql % Provided,
    libraryDependencies += jgit % Compile,
    libraryDependencies += siva % Compile,
    libraryDependencies += commonsIO % Compile,

    resolvers += "jitpack" at "https://jitpack.io",

    test in assembly := {},
    assemblyJarName in assembly := s"${name.value}-uber.jar"
  )

jacoco.settings

jacoco.reportFormats in jacoco.Config := Seq(
  XMLReport(encoding = "utf-8"))

parallelExecution in Test := false
logBuffered in Test := false
