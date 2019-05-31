import de.heikoseeberger.sbtheader.HeaderPattern
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

val AkkaVersion = "2.5.23"

val akkaPersistenceCassandraDependencies = Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"               % "3.2.0",
  "com.typesafe.akka"      %% "akka-persistence"                    % AkkaVersion,
  "com.typesafe.akka"      %% "akka-cluster-tools"                  % AkkaVersion,
  "com.typesafe.akka"      %% "akka-persistence-query"              % AkkaVersion,
  "com.typesafe.akka"      %% "akka-persistence-tck"                % AkkaVersion     % "test",
  "com.typesafe.akka"      %% "akka-stream-testkit"                 % AkkaVersion     % "test",
  "org.scalatest"          %% "scalatest"                           % "3.0.0"         % "test",
  "org.osgi"                % "org.osgi.core"                       % "5.0.0"         % "provided"
)

def common: Seq[Setting[_]] = SbtScalariform.scalariformSettings ++ Seq(
  organization := "com.typesafe.akka",
  organizationName := "Typesafe Inc.",
  licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),

  crossScalaVersions := Seq("2.11.12", "2.12.8", "2.13.0-RC2"),
  scalaVersion := crossScalaVersions.value.head,
  crossVersion := CrossVersion.binary,

  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-deprecation",
    //"-Xfatal-warnings",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen"
  ),

  scalacOptions ++= {
    // define scalac options that are only valid or desirable for 2.11 and 2.12
    if (scalaVersion.value.startsWith("2.13")) Seq.empty
    else
      Seq(
        "-Xfuture"// invalid in 2.13
      )
  },

  headers := headers.value ++ Map(
    "scala" -> (
      HeaderPattern.cStyleBlockComment,
      """|/*
         | * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
         | */
         |""".stripMargin
    )
  ),

  releaseCrossBuild := true,

  logBuffered in Test := System.getProperty("akka.logBufferedTests", "false").toBoolean,

  // show full stack traces and test case durations
  testOptions in Test += Tests.Argument("-oDF"),

  // don't save test output to a file
  testListeners in (Test, test) := Seq(TestLogger(streams.value.log, {_ => streams.value.log }, logBuffered.value)),

  // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
  // -a Show stack traces and exception class name for AssertionErrors.
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),

  // disable parallel tests
  parallelExecution in Test := false,

  ScalariformKeys.preferences in Compile  := formattingPreferences,
  ScalariformKeys.preferences in Test     := formattingPreferences
)

lazy val root = (project in file("."))
  .aggregate(core, cassandraLauncher)
  .settings(common: _*)
  .settings(
    name := "akka-peristence-cassandra-root",

    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
    publish := {},
    PgpKeys.publishSigned := {}
  )

lazy val core = (project in file("core"))
  .enablePlugins(AutomateHeaderPlugin, SbtOsgi)
  .dependsOn(cassandraLauncher % Test)
  .settings(common: _*)
  .settings(osgiSettings: _*)
  .settings(
    name := "akka-persistence-cassandra",
    libraryDependencies ++= akkaPersistenceCassandraDependencies,

    OsgiKeys.exportPackage  := Seq("akka.persistence.cassandra.*"),
    OsgiKeys.importPackage  := Seq(akkaImport(), optionalImport("org.apache.cassandra.*"), "*"),
    OsgiKeys.privatePackage := Nil
  )

lazy val cassandraLauncher = (project in file("cassandra-launcher"))
  .settings(common: _*)
  .settings(
    name := "akka-persistence-cassandra-launcher",
    managedResourceDirectories in Compile += (target in cassandraBundle).value / "bundle",
    managedResources in Compile += (assembly in cassandraBundle).value
  )

// This project doesn't get published directly, rather the assembled artifact is included as part of cassandraLaunchers
// resources
lazy val cassandraBundle = (project in file("cassandra-bundle"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "akka-persistence-cassandra-bundle",
    crossPaths := false,
    autoScalaLibrary := false,
    libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "3.11.2" exclude("commons-logging", "commons-logging"),

    target in assembly := target.value / "bundle" / "akka" / "persistence" / "cassandra" / "launcher",
    assemblyJarName in assembly := "cassandra-bundle.jar"
  )

def formattingPreferences = {
  import scalariform.formatter.preferences._
  FormattingPreferences()
    .setPreference(RewriteArrowSymbols, false)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(SpacesAroundMultiImports, true)
}

def akkaImport(packageName: String = "akka.*") = versionedImport(packageName, "2.4", "2.5")
def configImport(packageName: String = "com.typesafe.config.*") = versionedImport(packageName, "1.3.0", "1.4.0")
def versionedImport(packageName: String, lower: String, upper: String) = s"""$packageName;version="[$lower,$upper)""""
def optionalImport(packageName: String) = s"$packageName;resolution:=optional"
