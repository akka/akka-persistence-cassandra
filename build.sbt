organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.4-SNAPSHOT"

scalaVersion := "2.11.6"

fork in Test := true

javaOptions in Test += "-Xmx2500M"

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

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"             % "2.1.5",
  "com.typesafe.akka"      %% "akka-persistence"                  % "2.4.0-RC2",
  "com.typesafe.akka"      %% "akka-persistence-tck"              % "2.4.0-RC2"  % "test",
  "org.scalatest"          %% "scalatest"                         % "2.1.4"      % "test",
  // override cassandra unit cassandra version as there is a bug with static columns in 2.1.3
  // remove once PR https://github.com/jsevellec/cassandra-unit/pull/141 merged/released
  "org.apache.cassandra"    % "cassandra-all"                     % "2.1.8"      % "test",
  "org.cassandraunit"       % "cassandra-unit"                    % "2.1.3.1"    % "test"
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
