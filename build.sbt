import java.nio.file.{Files, StandardCopyOption}

import Dependencies.{scalaTest, _}

organization := "tech.sourced"
scalaVersion := "2.11.11"
version := "0.1.9"
name := "engine"

libraryDependencies += scalaTest % Test
libraryDependencies += scoverage % Test
libraryDependencies += sparkSql % Provided
libraryDependencies += newerHadoopClient % Provided //due to newer v. of guava in bblfsh
// grpc for bblfsh/client-scala needs to be newer then in Spark
libraryDependencies += fixNettyForGrpc
libraryDependencies += jgit % Compile
libraryDependencies += siva % Compile
libraryDependencies += bblfsh % Compile
libraryDependencies += commonsIO % Compile
libraryDependencies += commonsPool % Compile
libraryDependencies += enry % Compile
libraryDependencies += scalaLib % Provided

inThisBuild(Testing.settings)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oUT")

test in assembly := {}
assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

parallelExecution in Test := false
logBuffered in Test := false

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "com.google.shadedcommon.@1").inAll,
  ShadeRule.rename("io.netty.**" -> "io.shadednetty.@1").inAll,
  ShadeRule.rename("org.apache.commons.**" -> "org.apache.shadedcommons.@1").inAll,
  ShadeRule.rename("org.eclipse.jgit.**" -> "org.eclipse.shadedjgit.@1").inAll
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

pomPostProcess := { (node: scala.xml.Node) =>
  import scala.xml._
  import scala.xml.transform._

  object DependencyEraser extends RewriteRule {
    override def transform(n: Node): Seq[Node] = n match {
      case e: Elem if e.label == "dependencies" =>
        <dependencies>
          {e.child}<dependency>
          <groupId>org.scala-lang</groupId>
          <artifactId>scala-library</artifactId>
          <version>
            {scalaVersion.value}
          </version>
          <scope>provided</scope>
        </dependency>
        </dependencies>
      case e: Elem if e.label == "dependency"
        && e.child.exists(child => child.label == "scope" && child.text == "provided") =>
        e
      case e: Elem if e.label == "dependency" =>
        val organization = e.child.filter(_.label == "groupId").flatMap(_.text).mkString
        val artifact = e.child.filter(_.label == "artifactId").flatMap(_.text).mkString
        val version = e.child.filter(_.label == "version").flatMap(_.text).mkString
        Comment(s" not provided dependency $organization#$artifact;$version has been omitted ")
      case other => other
    }
  }

  object eraseDependencies extends RuleTransformer(DependencyEraser)

  eraseDependencies(node)
}

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

publishArtifact in(Compile, packageBin) := false

assembly := {
  val file = assembly.value
  val dest = new java.io.File(file.getParent, s"${name.value}-uber.jar")
  Files.copy(
    new java.io.File(file.getAbsolutePath).toPath,
    dest.toPath,
    StandardCopyOption.REPLACE_EXISTING
  )
  file
}

addArtifact(artifact in(Compile, assembly), assembly)

isSnapshot := version.value endsWith "SNAPSHOT"

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  } else {
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
}
