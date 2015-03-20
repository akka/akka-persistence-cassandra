organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.3.7"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.10.4", "2.11.6")

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
  "com.typesafe.akka"      %% "akka-persistence-experimental"     % "2.3.9",
  "com.typesafe.akka"      %% "akka-persistence-tck-experimental" % "2.3.9"   % "test",
  "org.scalatest"          %% "scalatest"                         % "2.1.4"   % "test",
  "org.cassandraunit"       % "cassandra-unit"                    % "2.0.2.2" % "test"
)

