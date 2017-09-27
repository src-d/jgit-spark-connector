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
    libraryDependencies += fixNettyForGrpc, // grpc for bblfsh/client-scala needs to be newer then in Spark
    libraryDependencies += jgit % Compile,
    libraryDependencies += siva % Compile,
    libraryDependencies += bblfsh % Compile,
    libraryDependencies += commonsIO % Compile,
    libraryDependencies += enry % Compile,

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

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "com.google.shadedcommon.@1").inAll,
  ShadeRule.rename("io.netty.**" -> "io.shadednetty.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
