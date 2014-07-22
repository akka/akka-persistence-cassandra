organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.3.3"

scalaVersion := "2.11.0"

crossScalaVersions := Seq("2.10.4", "2.11.0")

parallelExecution in Test := false

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.3" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3.4" % "compile"

libraryDependencies += "com.github.krasserm" %% "akka-persistence-testkit" % "0.3.3" % "test"
