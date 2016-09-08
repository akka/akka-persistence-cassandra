import Tests._
import de.heikoseeberger.sbtheader.HeaderPattern
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

enablePlugins(AutomateHeaderPlugin)

organization := "com.typesafe.akka"
organizationName := "Typesafe Inc."

name := "akka-persistence-cassandra"

licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))

scalaVersion := "2.11.8"
crossVersion := CrossVersion.binary

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

// group tests, a single test per group
def singleTests(tests: Seq[TestDefinition]) = {
  // We could group non Cassandra tests into another group
  // to avoid new JVM for each test, see http://www.scala-sbt.org/release/docs/Testing.html
  val javaOptions = Seq("-Xms512M", "-Xmx1G", "-XX:+PrintGCDetails", "-XX:+PrintGCTimeStamps")
  tests map { test =>
    new Group(
      name = test.name,
      tests = Seq(test),
      runPolicy = SubProcess(javaOptions))
  }
}

javaOptions in Test ++= Seq("-Xms512M", "-Xmx1G", "-XX:+PrintGCDetails", "-XX:+PrintGCTimeStamps")

fork in Test := true // for Cassandra tests

testGrouping in Test <<= definedTests in Test map singleTests // for Cassandra tests

// show full stack traces and test case durations
testOptions in Test += Tests.Argument("-oDF")

// -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
// -a Show stack traces and exception class name for AssertionErrors.
testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")

// disable parallel tests
parallelExecution in Test := false

val AkkaVersion = "2.4.10"

libraryDependencies ++= Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"               % "3.0.0",
  "com.typesafe.akka"      %% "akka-persistence"                    % AkkaVersion,
  "com.typesafe.akka"      %% "akka-cluster-tools"                  % AkkaVersion,
  "com.typesafe.akka"      %% "akka-persistence-query-experimental" % AkkaVersion,
  "com.typesafe.akka"      %% "akka-persistence-tck"                % AkkaVersion   % "test",
  "com.typesafe.akka"      %% "akka-stream-testkit"                 % AkkaVersion   % "test",
  "org.scalatest"          %% "scalatest"                           % "2.1.4"       % "test",
  // cassandra-all for testkit.CassandraLauncher, app should define it as test dependency if needed
  "org.apache.cassandra"    % "cassandra-all"                       % "3.0.2"      % "optional"
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
