
name := "spark"

organization := "tech.sourced"

libraryDependencies ++= Seq(
  "tech.sourced" %% "core" % "0.0.1",
  "org.apache.spark" %% "spark-sql" % "2.2.0"
)
