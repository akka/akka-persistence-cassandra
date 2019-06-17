import sbt._
import Keys._

object Dependencies {
  val Scala212 = "2.12.8"
  val Scala213 = "2.13.0"
  val ScalaVersions = Seq(Scala212, Scala213)

  val AkkaVersion = "2.5.23"
  val CassandraVersionInDocs = "4.0"

  val akkaCassandraSessionDependencies = Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.7.1",
    "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
    "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test)

  val akkaPersistenceCassandraDependencies = Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.7.1",
    // Specifying guava dependency because older transitive dependency has security vulnerability
    "com.google.guava" % "guava" % "27.0.1-jre",
    // Specifying jnr-posix version for licensing reasons: cassandra-driver-core
    // depends on version 3.0.44, but for this version the LICENSE.txt and the
    // pom.xml have conflicting licensing information. 3.0.45 fixes this and
    // makes it clear this library is available under (among others) the EPL
    "com.github.jnr" % "jnr-posix" % "3.0.45",
    "com.typesafe.akka" %% "akka-persistence" % AkkaVersion,
    "com.typesafe.akka" %% "akka-cluster-tools" % AkkaVersion,
    "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion,
    "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-cluster-typed" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-multi-node-testkit" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-cluster-sharding" % AkkaVersion % Test,
    "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
    "org.scalatest" %% "scalatest" % "3.0.8-RC2" % Test,
    "org.pegdown" % "pegdown" % "1.6.0" % Test,
    "org.osgi" % "org.osgi.core" % "5.0.0" % Provided)
}
