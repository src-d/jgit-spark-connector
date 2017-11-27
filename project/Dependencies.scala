import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"
  lazy val scoverage = "org.scoverage" %% "scalac-scoverage-plugin" % "1.3.1"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.2.0"
  lazy val newerHadoopClient = "org.apache.hadoop" % "hadoop-client" % "2.7.2"
  lazy val fixNettyForGrpc = "io.netty" % "netty-all" % "4.1.11.Final"
  lazy val jgit = "org.eclipse.jgit" % "org.eclipse.jgit" % "4.9.0.201710071750-r"
  lazy val siva = "tech.sourced" % "siva-java" % "0.1.1"
  lazy val bblfsh = "org.bblfsh" % "bblfsh-client" % "1.3.3"
  lazy val enry = "tech.sourced" % "enry-java" % "1.6.1"
  lazy val commonsIO = "commons-io" % "commons-io" % "2.5"
  lazy val commonsPool = "org.apache.commons" % "commons-pool2" % "2.4.3"
  lazy val scalaLib = "org.scala-lang" % "scala-library" % "2.11.11"
}
