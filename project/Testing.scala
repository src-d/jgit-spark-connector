import sbt._
import sbt.Keys._

object Testing {
  lazy val Integration = config("integration").extend(Runtime)

  lazy val settings: Seq[Def.Setting[_]] =
    inConfig(Integration)(Defaults.testSettings) ++
      Seq(
        fork in IntegrationTest := false,
        parallelExecution in IntegrationTest := false,
        scalaSource in IntegrationTest := baseDirectory.value / "src/integration/scala")
}