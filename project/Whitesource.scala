import sbt._
import sbt.Keys._
import sbtwhitesource.WhiteSourcePlugin.autoImport._
import sbtwhitesource._

import scala.sys.process.Process

object Whitesource extends AutoPlugin {
  override def requires = WhiteSourcePlugin

  override def trigger = allRequirements

  override lazy val projectSettings = Seq(
    // do not change the value of whitesourceProduct
    whitesourceProduct := "Lightbend Reactive Platform",
    whitesourceAggregateProjectName := {
      val projectName =
        (moduleName in LocalRootProject).value.replace("-root", "")
      projectName + "-" + (if (isSnapshot.value)
                             if (describe(baseDirectory.value) contains "master") "master"
                             else "adhoc"
                           else
                             CrossVersion
                               .partialVersion((version in LocalRootProject).value)
                               .map {
                                 case (major, minor) => s"$major.$minor-stable"
                               }
                               .getOrElse("adhoc"))
    },
    whitesourceForceCheckAllDependencies := true,
    whitesourceFailOnError := true)

  private def describe(base: File) = Process(Seq("git", "describe", "--all"), base).!!
}
