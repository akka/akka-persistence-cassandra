import com.lightbend.paradox.projectinfo.ParadoxProjectInfoPluginKeys._
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import de.heikoseeberger.sbtheader._
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin
import xerial.sbt.Sonatype.autoImport.sonatypeProfileName

object Common extends AutoPlugin {

  override def trigger = allRequirements

  override def requires = JvmPlugin && HeaderPlugin

  override def globalSettings =
    Seq(
      organization := "com.typesafe.akka",
      organizationName := "Lightbend Inc.",
      organizationHomepage := Some(url("https://www.lightbend.com/")),
      startYear := Some(2016),
      homepage := Some(url("https://akka.io")),
      // apiURL defined in projectSettings because version.value is not correct here
      scmInfo := Some(
          ScmInfo(
            url("https://github.com/akka/akka-persistence-cassandra"),
            "git@github.com:akka/akka-persistence-cassandra.git")),
      developers += Developer(
          "contributors",
          "Contributors",
          "https://gitter.im/akka/dev",
          url("https://github.com/akka/akka-persistence-cassandra/graphs/contributors")),
      releaseNotesURL := (
        if ((ThisBuild / isSnapshot).value) None
        else Some(url(s"https://github.com/akka/akka-persistence-cassandra/releases/tag/v${version.value}"))
        ),
      licenses := {
        val tagOrBranch =
          if (version.value.endsWith("SNAPSHOT")) "main"
          else "v" + version.value
        Seq(
          (
            "BUSL-1.1",
            url(s"https://raw.githubusercontent.com/akka/akka-persistence-cassandra/${tagOrBranch}/LICENSE")))
      },
      description := "A Cassandra plugin for Akka Persistence.")

  override lazy val projectSettings = Seq(
    projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value),
    crossVersion := CrossVersion.binary,
    crossScalaVersions := Dependencies.Scala2Versions,
    scalaVersion := Dependencies.Scala213,
    scalacOptions ++= Seq("-encoding", "UTF-8", "-feature", "-unchecked", "-Xlint", "-Ywarn-dead-code", "-deprecation"),
    Compile / console / scalacOptions --= Seq("-deprecation", "-Xfatal-warnings", "-Xlint", "-Ywarn-unused:imports"),
    Compile / doc / scalacOptions := scalacOptions.value ++ Seq(
        "-doc-title",
        "Akka Persistence Cassandra",
        "-doc-version",
        version.value,
        "-sourcepath",
        (ThisBuild / baseDirectory).value.toString,
        "-doc-source-url", {
          val branch = if (isSnapshot.value) "main" else s"v${version.value}"
          s"https://github.com/akka/akka-persistence-cassandra/tree/${branch}€{FILE_PATH_EXT}#L€{FILE_LINE}"
        },
        "-doc-canonical-base-url",
        "https://doc.akka.io/api/akka-persistence-cassandra/current/")
      ++ {
        if (scalaBinaryVersion.value.startsWith("3")) {
          Seq("-skip-packages:akka.pattern") // different usage in scala3
        } else {
          Seq("-skip-packages", "akka.pattern") // for some reason Scaladoc creates this
        }
      },
    Compile / doc / scalacOptions --= Seq("-Xfatal-warnings"),
    scalafmtOnCompile := true,
    autoAPIMappings := true,
    apiURL := Some(url(s"https://doc.akka.io/api/akka-persistence-cassandra/${projectInfoVersion.value}")),
    headerLicense := Some(
        HeaderLicense.Custom("""Copyright (C) 2016-2023 Lightbend Inc. <https://www.lightbend.com>""")),
    sonatypeProfileName := "com.typesafe",
    Test / logBuffered := System.getProperty("akka.logBufferedTests", "false").toBoolean,
    // show full stack traces and test case durations
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
    // -a Show stack traces and exception class name for AssertionErrors.
    // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
    // -q Suppress stdout for successful tests.
    Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-q"),
    Test / parallelExecution := false)
}
