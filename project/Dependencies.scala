import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.2.0"
  lazy val newerHadoopClient = "org.apache.hadoop" % "hadoop-client" % "2.7.2"
  lazy val fixNettyForGrpc = "io.netty" % "netty-all" % "4.1.11.Final"
  lazy val jgit = "org.eclipse.jgit" % "org.eclipse.jgit" % "4.8.0.201706111038-r"
  lazy val siva = "com.github.src-d" % "siva-java" % "master-SNAPSHOT"
  lazy val bblfsh = "com.github.bblfsh" % "client-scala" % "master-SNAPSHOT"
  lazy val commonsIO = "commons-io" % "commons-io" % "2.5"
}
