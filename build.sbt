organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.1-SNAPSHOT"

scalaVersion := "2.10.2"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.0-rc2" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3-M2" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.3-M2" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"

libraryDependencies += "commons-io" % "commons-io" % "2.4" % "test"
