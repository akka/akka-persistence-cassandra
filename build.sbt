organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.3.3"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.10.4", "2.11.0")

fork in Test := true

javaOptions in Test += "-Xmx2500M"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"             % "2.1.1",
  "com.typesafe.akka"      %% "akka-persistence-experimental"     % "2.3.6",
  "com.typesafe.akka"      %% "akka-persistence-tck-experimental" % "2.3.6"   % "test",
  "org.scalatest"          %% "scalatest"                         % "2.1.4"   % "test",
  "org.cassandraunit"       % "cassandra-unit"                    % "2.0.2.1" % "test"
)

