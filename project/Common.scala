import com.lightbend.paradox.projectinfo.ParadoxProjectInfoPluginKeys._
import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import de.heikoseeberger.sbtheader._
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin
import sbtrelease.ReleasePlugin.autoImport.releaseCrossBuild

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
      apiURL := Some(url(s"https://doc.akka.io/api/akka-persistence-cassandra/${version.value}")),
      scmInfo := Some(
          ScmInfo(
            url("https://github.com/akka/akka-persistence-cassandra"),
            "git@github.com:akka/akka-persistence-cassandra.git")),
      developers += Developer(
          "contributors",
          "Contributors",
          "https://gitter.im/akka/dev",
          url("https://github.com/akka/akka-persistence-cassandra/graphs/contributors")),
      licenses := Seq(("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))),
      description := "A Cassandra plugin for Akka Persistence.")

  override lazy val projectSettings = Seq(
    projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value),
    crossVersion := CrossVersion.binary,
    crossScalaVersions := Dependencies.ScalaVersions,
    scalaVersion := Dependencies.Scala212,
    scalacOptions ++= Seq("-encoding", "UTF-8", "-feature", "-unchecked", "-Xlint", "-Ywarn-dead-code", "-deprecation"),
    scalacOptions ++= {
      // define scalac options that are only valid or desirable for 2.12
      if (scalaVersion.value.startsWith("2.13"))
        Seq()
      else
        Seq(
          // -deprecation causes some warnings on 2.13 because of collection converters.
          // We only enable `fatal-warnings` on 2.12 and accept the warning on 2.13
          "-Xfatal-warnings",
          "-Xfuture" // invalid in 2.13
        )
    },
    Compile / console / scalacOptions --= Seq("-deprecation", "-Xfatal-warnings", "-Xlint", "-Ywarn-unused:imports"),
    Compile / doc / scalacOptions := scalacOptions.value ++ Seq(
        "-doc-title",
        "Akka Persistence Cassandra",
        "-doc-version",
        version.value,
        "-sourcepath",
        (baseDirectory in ThisBuild).value.toString,
        "-doc-source-url", {
          val branch = if (isSnapshot.value) "master" else s"v${version.value}"
          s"https://github.com/akka/akka-persistence-cassandra-1.x/tree/${branch}€{FILE_PATH}.scala#L1"
        },
        "-skip-packages",
        "akka.pattern" // for some reason Scaladoc creates this
      ),
    Compile / doc / scalacOptions --= Seq("-Xfatal-warnings"),
    scalafmtOnCompile := true,
    releaseCrossBuild := true,
    autoAPIMappings := true,
    headerLicense := Some(
        HeaderLicense.Custom("""Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>""")),
    Test / logBuffered := System.getProperty("akka.logBufferedTests", "false").toBoolean,
    // show full stack traces and test case durations
    Test / testOptions += Tests.Argument("-oDF"),
    // -a Show stack traces and exception class name for AssertionErrors.
    // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
    // -q Suppress stdout for successful tests.
    Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-q"),
    Test / parallelExecution := false)
}
