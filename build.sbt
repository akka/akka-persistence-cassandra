import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

val AkkaVersion = "2.5.19"

val akkaPersistenceCassandraDependencies = Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"               % "3.6.0",
  // Specifying guava dependency because older transitive dependency has security vulnerability
  "com.google.guava"        % "guava"                               % "27.0.1-jre",
  "com.typesafe.akka"      %% "akka-persistence"                    % AkkaVersion,
  "com.typesafe.akka"      %% "akka-cluster-tools"                  % AkkaVersion,
  "com.typesafe.akka"      %% "akka-persistence-query"              % AkkaVersion,
  "com.typesafe.akka"      %% "akka-persistence-tck"                % AkkaVersion     % Test,
  "com.typesafe.akka"      %% "akka-stream-testkit"                 % AkkaVersion     % Test,
  "com.typesafe.akka"      %% "akka-multi-node-testkit"             % AkkaVersion     % Test,
  "ch.qos.logback"          % "logback-classic"                     % "1.2.3"         % Test,
  "org.scalatest"          %% "scalatest"                           % "3.0.5"         % Test,
  "org.pegdown"             % "pegdown"                             % "1.6.0"         % Test,
  "org.osgi"                % "org.osgi.core"                       % "5.0.0"         % Provided
)


def common: Seq[Setting[_]] = SbtScalariform.scalariformSettings ++ Seq(
  organization := "com.typesafe.akka",
  organizationName := "Lightbend Inc.",
  startYear := Some(2016),
  licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
  crossScalaVersions := Seq("2.11.12", "2.12.7"),
  scalaVersion := crossScalaVersions.value.last,
  crossVersion := CrossVersion.binary,

  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-deprecation",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Xfuture"
  ),

  headerLicense := Some(HeaderLicense.Custom(
    """Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>"""
  )),

  releaseCrossBuild := true,

  logBuffered in Test := System.getProperty("akka.logBufferedTests", "false").toBoolean,

  // show full stack traces and test case durations
  testOptions in Test += Tests.Argument("-oDF"),

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
    name := "akka-persistence-cassandra-root",

    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
    publish := {},
    PgpKeys.publishSigned := {}
  )

lazy val core = (project in file("core"))
  .enablePlugins(AutomateHeaderPlugin, SbtOsgi, MultiJvmPlugin)
  .dependsOn(cassandraLauncher % Test)
  .settings(common: _*)
  .settings(osgiSettings: _*)
  .settings(
    name := "akka-persistence-cassandra",
    libraryDependencies ++= akkaPersistenceCassandraDependencies,

    OsgiKeys.exportPackage  := Seq("akka.persistence.cassandra.*"),
    OsgiKeys.importPackage  := Seq(akkaImport(), optionalImport("org.apache.cassandra.*"), "*"),
    OsgiKeys.privatePackage := Nil,
    testOptions in Test ++= Seq(Tests.Argument(TestFrameworks.ScalaTest, "-o"), Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports"))
  )
  .configs( MultiJvm)

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
  .settings(common: _*)
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
