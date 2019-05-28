import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

val AkkaVersion = "2.5.23"

val akkaPersistenceCassandraDependencies = Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.7.1",
  // Specifying guava dependency because older transitive dependency has security vulnerability
  "com.google.guava" % "guava" % "27.0.1-jre",
  // Specifying jnr-posix version for licensing reasons: cassandra-driver-core
  // depends on version 3.0.44, but for this version the LICENSE.txt and the
  // pom.xml have conflicting licensing information. 3.0.45 fixes this and
  // makes it clear this library is available under (among others) the EPL
  "com.github.jnr" % "jnr-posix" % "3.0.45",
  "com.typesafe.akka" %% "akka-persistence" % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % AkkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion,
  "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion % Test,
  "com.typesafe.akka" %% "akka-cluster-typed" % AkkaVersion % Test,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
  "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
  "com.typesafe.akka" %% "akka-multi-node-testkit" % AkkaVersion % Test,
  "com.typesafe.akka" %% "akka-cluster-sharding" % AkkaVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8-RC2" % Test,
  "org.pegdown" % "pegdown" % "1.6.0" % Test,
  "org.osgi" % "org.osgi.core" % "5.0.0" % Provided)

def common: Seq[Setting[_]] = Seq(
  organization := "com.typesafe.akka",
  organizationName := "Lightbend Inc.",
  startYear := Some(2016),
  licenses := Seq(("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))),
  crossScalaVersions := Seq("2.11.12", "2.13.0-RC2", "2.12.8"),
  scalaVersion := crossScalaVersions.value.last,
  crossVersion := CrossVersion.binary,
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-feature",
    "-unchecked",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Xfatal-warnings"
  ),
  scalacOptions ++= {
    // define scalac options that are only valid or desirable for 2.11 and 2.12
    if (scalaVersion.value.startsWith("2.13")) Seq.empty
    else 
    Seq(
      "-Xfuture", // invalid in 2.13
      "-deprecation" // temporarily allowing deprecation because of Java colletion coverters
    )
  },
  Compile / console / scalacOptions --= Seq("-deprecation", "-Xfatal-warnings", "-Xlint", "-Ywarn-unused:imports"),
  Compile / doc / scalacOptions --= Seq("-Xfatal-warnings"),
  headerLicense := Some(
    HeaderLicense.Custom(
      """Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>"""
    )),
  scalafmtOnCompile := true,
  releaseCrossBuild := true,
  logBuffered in Test := System.getProperty("akka.logBufferedTests", "false").toBoolean,
  // show full stack traces and test case durations
  testOptions in Test += Tests.Argument("-oDF"),
  // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
  // -a Show stack traces and exception class name for AssertionErrors.
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
  // disable parallel tests
  parallelExecution in Test := false)



lazy val root = (project in file("."))
  .aggregate(core, cassandraLauncher)
  .settings(common: _*)
  .settings(
    name := "akka-persistence-cassandra-root",
    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
    publish := {},
    PgpKeys.publishSigned := {})

lazy val core = (project in file("core"))
  .enablePlugins(AutomateHeaderPlugin, SbtOsgi, MultiJvmPlugin)
  .dependsOn(cassandraLauncher % Test)
  .settings(common: _*)
  .settings(osgiSettings: _*)
  .settings({
    val silencerVersion = "1.4.0"
    Seq(
      libraryDependencies ++= Seq(
        compilerPlugin("com.github.ghik" %% "silencer-plugin" % silencerVersion),
        "com.github.ghik" %% "silencer-lib" % silencerVersion % Provided),
      // Hack because 'provided' dependencies by default are not picked up by the multi-jvm plugin:
      managedClasspath in MultiJvm ++= (managedClasspath in Compile).value.filter(_.data.name.contains("silencer-lib"))
      )
  })
  .settings(
    name := "akka-persistence-cassandra",
    libraryDependencies ++= akkaPersistenceCassandraDependencies,
    OsgiKeys.exportPackage := Seq("akka.persistence.cassandra.*"),
    OsgiKeys.importPackage := Seq(akkaImport(), optionalImport("org.apache.cassandra.*"), "*"),
    OsgiKeys.privatePackage := Nil,
    testOptions in Test ++= Seq(
        Tests.Argument(TestFrameworks.ScalaTest, "-o"),
        Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports")))
  .configs(MultiJvm)

lazy val cassandraLauncher = (project in file("cassandra-launcher"))
  .settings(common: _*)
  .settings(
    name := "akka-persistence-cassandra-launcher",
    managedResourceDirectories in Compile += (target in cassandraBundle).value / "bundle",
    managedResources in Compile += (assembly in cassandraBundle).value)

// This project doesn't get published directly, rather the assembled artifact is included as part of cassandraLaunchers
// resources
lazy val cassandraBundle = (project in file("cassandra-bundle"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(common: _*)
  .settings(
    name := "akka-persistence-cassandra-bundle",
    crossPaths := false,
    autoScalaLibrary := false,
    libraryDependencies += ("org.apache.cassandra" % "cassandra-all" % "3.11.3")
        .exclude("commons-logging", "commons-logging"),
    dependencyOverrides += "com.github.jbellis" % "jamm" % "0.3.3", // See jamm comment in https://issues.apache.org/jira/browse/CASSANDRA-9608
    target in assembly := target.value / "bundle" / "akka" / "persistence" / "cassandra" / "launcher",
    assemblyJarName in assembly := "cassandra-bundle.jar")

def akkaImport(packageName: String = "akka.*") =
  versionedImport(packageName, "2.4", "2.5")
def configImport(packageName: String = "com.typesafe.config.*") =
  versionedImport(packageName, "1.3.0", "1.4.0")
def versionedImport(packageName: String, lower: String, upper: String) =
  s"""$packageName;version="[$lower,$upper)""""
def optionalImport(packageName: String) = s"$packageName;resolution:=optional"
