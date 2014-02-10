organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.2-SNAPSHOT"

scalaVersion := "2.10.3"

parallelExecution in Test := false

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.0-rc2" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3.0-RC2" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.3.0-RC2" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"

libraryDependencies += "commons-io" % "commons-io" % "2.4" % "test"
