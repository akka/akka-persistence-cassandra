import Tests._
import de.heikoseeberger.sbtheader.HeaderPattern
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

enablePlugins(AutomateHeaderPlugin,SbtOsgi)

organization := "com.typesafe.akka"
organizationName := "Typesafe Inc."

name := "akka-persistence-cassandra"

licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))

crossScalaVersions := Seq("2.11.8", "2.12.1")
scalaVersion := crossScalaVersions.value.head
crossVersion := CrossVersion.binary
releaseCrossBuild := true

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-deprecation",
  //"-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture"
)


// show full stack traces and test case durations
testOptions in Test += Tests.Argument("-oDF")

// -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
// -a Show stack traces and exception class name for AssertionErrors.
testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")

// disable parallel tests
parallelExecution in Test := false

val AkkaVersion = "2.5.0"

libraryDependencies ++= Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"               % "3.1.0",
  "com.typesafe.akka"      %% "akka-persistence"                    % AkkaVersion,
  "com.typesafe.akka"      %% "akka-cluster-tools"                  % AkkaVersion,
  "com.typesafe.akka"      %% "akka-persistence-query"              % AkkaVersion,
  "com.typesafe.akka"      %% "akka-persistence-tck"                % AkkaVersion     % "test",
  "com.typesafe.akka"      %% "akka-stream-testkit"                 % AkkaVersion     % "test",
  "org.scalatest"          %% "scalatest"                           % "3.0.0"         % "test",
  // cassandra-all for testkit.CassandraLauncher, app should define it as test dependency if needed
  "org.apache.cassandra"    % "cassandra-all"                       % "3.10"          % "optional" exclude("io.netty", "netty-all"),
  // cassandra-all 3.10 depends on netty-all 4.0.39, while cassandra-driver-core 3.1.0 depends on netty-handler 4.0.37,
  // we exclude netty-all, and upgrade individual deps
  "io.netty"                % "netty-handler"                       % "4.0.39.Final"  % "optional",
  "io.netty"                % "netty-transport-native-epoll"        % "4.0.39.Final"  % "optional",
  "org.osgi"                % "org.osgi.core"                       % "5.0.0"         % "provided"
)

headers := headers.value ++ Map(
  "scala" -> (
    HeaderPattern.cStyleBlockComment,
    """|/*
       | * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
       | */
       |""".stripMargin
  )
)

SbtScalariform.scalariformSettings
ScalariformKeys.preferences in Compile  := formattingPreferences
ScalariformKeys.preferences in Test     := formattingPreferences

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

osgiSettings
OsgiKeys.exportPackage  := Seq("akka.persistence.cassandra.*")
OsgiKeys.importPackage  := Seq(akkaImport(), optionalImport("org.apache.cassandra.*"), "*")
OsgiKeys.privatePackage := Seq()
