import Dependencies.{scalaTest, _}
import sbt.Keys.{libraryDependencies, resolvers}

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "tech.sourced",
      scalaVersion := "2.11.11",
      version := "0.1.3"
    )),
    name := "engine",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += scoverage % Test,
    libraryDependencies += sparkSql % Provided,
    libraryDependencies += newerHadoopClient % Provided, //due to newer v. of guava in bblfsh
    // grpc for bblfsh/client-scala needs to be newer then in Spark
    libraryDependencies += fixNettyForGrpc,
    libraryDependencies += jgit % Compile,
    libraryDependencies += siva % Compile,
    libraryDependencies += bblfsh % Compile,
    libraryDependencies += commonsIO % Compile,
    libraryDependencies += enry % Compile,

    test in assembly := {},
    assemblyJarName in assembly := s"${name.value}-uber.jar"
  )

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

sonatypeProfileName := "tech.sourced"

// pom settings for sonatype
homepage := Some(url("https://github.com/src-d/engine"))
scmInfo := Some(ScmInfo(url("https://github.com/src-d/engine"),
  "git@github.com:src-d/engine.git"))
developers += Developer("ajnavarro",
  "Antonio Navarro",
  "antonio@sourced.tech",
  url("https://github.com/ajnavarro"))
developers += Developer("bzz",
  "Alexander Bezzubov",
  "alex@sourced.tech",
  url("https://github.com/bzz"))
developers += Developer("mcarmonaa",
  "Manuel Carmona",
  "manuel@sourced.tech",
  url("https://github.com/mcarmonaa"))
developers += Developer("erizocosmico",
  "Miguel Molina",
  "miguel@sourced.tech",
  url("https://github.com/erizocosmico"))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
pomIncludeRepository := (_ => false)

crossPaths := false
publishMavenStyle := true

val SONATYPE_USERNAME = scala.util.Properties.envOrElse("SONATYPE_USERNAME", "NOT_SET")
val SONATYPE_PASSWORD = scala.util.Properties.envOrElse("SONATYPE_PASSWORD", "NOT_SET")
credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "oss.sonatype.org",
  SONATYPE_USERNAME,
  SONATYPE_PASSWORD)

val SONATYPE_PASSPHRASE = scala.util.Properties.envOrElse("SONATYPE_PASSPHRASE", "not set")

useGpg := false
pgpSecretRing := baseDirectory.value / "project" / ".gnupg" / "secring.gpg"
pgpPublicRing := baseDirectory.value / "project" / ".gnupg" / "pubring.gpg"
pgpPassphrase := Some(SONATYPE_PASSPHRASE.toArray)

isSnapshot := version.value endsWith "SNAPSHOT"

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  } else {
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
}
