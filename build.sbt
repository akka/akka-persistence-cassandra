organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.4-SNAPSHOT"

scalaVersion := "2.11.0"

crossScalaVersions := Seq("2.10.4", "2.11.0")

fork in Test := true

parallelExecution in Test := false

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

libraryDependencies ++= Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"         % "2.0.3",
  "com.typesafe.akka"      %% "akka-persistence-experimental" % "2.3.4",
  "com.github.krasserm"    %% "akka-persistence-testkit"      % "0.3.4"   % "test",
  "org.cassandraunit"       % "cassandra-unit"                % "2.0.2.1" % "test"
)

