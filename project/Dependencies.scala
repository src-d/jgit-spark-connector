import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"

  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.2.0"

  lazy val jgit = "org.eclipse.jgit" % "org.eclipse.jgit" % "4.8.0.201706111038-r"

}
