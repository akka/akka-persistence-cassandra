import Tests._

organization := "com.github.krasserm"

name := "akka-persistence-cassandra-3x"

version := "0.6"

scalaVersion := "2.11.7"

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
def singleTests(tests: Seq[TestDefinition]) =
  // We could group non Cassandra tests into another group
  // to avoid new JVM for each test, see http://www.scala-sbt.org/release/docs/Testing.html
  tests map { test =>
    new Group(
      name = test.name,
      tests = Seq(test),
      runPolicy = SubProcess(javaOptions = Seq.empty[String]))
  }

javaOptions in Test += "-Xmx2500M"

fork in Test := true // for Cassandra tests

testGrouping in Test <<= definedTests in Test map singleTests // for Cassandra tests

// show full stack traces and test case durations
testOptions in Test += Tests.Argument("-oDF")

// -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
// -a Show stack traces and exception class name for AssertionErrors.
testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a")

// disable parallel tests
parallelExecution in Test := false

val AkkaVersion = "2.4.1"

libraryDependencies ++= Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"               % "3.0.0-alpha5",
  "com.typesafe.akka"      %% "akka-persistence"                    % AkkaVersion,
  "com.typesafe.akka"      %% "akka-persistence-query-experimental" % AkkaVersion,
  "com.typesafe.akka"      %% "akka-persistence-tck"                % AkkaVersion      % "test",
  "org.scalatest"          %% "scalatest"                           % "2.1.4"      % "test",
  "com.typesafe.akka"      %% "akka-stream-testkit-experimental"    % "1.0"        % "test",
  // cassandra-all for testkit.CassandraLauncher, app should define it as test dependency if needed
  "org.apache.cassandra"    % "cassandra-all"                       % "3.0.0"      % "optional"
)

credentials += Credentials(
  "Artifactory Realm",
  "oss.jfrog.org",
  sys.env.getOrElse("OSS_JFROG_USER", ""),
  sys.env.getOrElse("OSS_JFROG_PASS", "")
)

publishTo := {
  val jfrog = "https://oss.jfrog.org/artifactory/"
  if (isSnapshot.value)
    Some("OJO Snapshots" at jfrog + "oss-snapshot-local")
  else
    Some("OJO Releases" at jfrog + "oss-release-local")
}

publishMavenStyle := true
