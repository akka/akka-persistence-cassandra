organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.3-SNAPSHOT"

scalaVersion := "2.10.3"

parallelExecution in Test := false

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.0.0-rc3" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3.0" % "compile"

libraryDependencies += "com.github.krasserm" %% "akka-persistence-testkit" % "0.2" % "test"

libraryDependencies += "commons-io" % "commons-io" % "2.4" % "test"
