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
    libraryDependencies += newerHadoopClient % Provided, //due to newer v. of guava in bblfsh
    libraryDependencies += jgit % Compile,
    libraryDependencies += siva % Compile,
    libraryDependencies += bblfsh % Compile,
    libraryDependencies += commonsIO % Compile,
    libraryDependencies += "tech.sourced" % "enry-java" % "1.0",

    resolvers += "jitpack" at "https://jitpack.io",
    // TODO: remove this local resolver when enry-java will be available from jitpack.
    resolvers += "Local Ivy repository" at "file://" + Path.userHome.absolutePath + "/.ivy2/repository",

    test in assembly := {},
    assemblyJarName in assembly := s"${name.value}-uber.jar"
  )

jacoco.settings

jacoco.reportFormats in jacoco.Config := Seq(
  XMLReport(encoding = "utf-8"))

parallelExecution in Test := false
logBuffered in Test := false
