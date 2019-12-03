import sbt._
import Keys._

object Dependencies {
  val Scala212 = "2.12.8"
  val Scala213 = "2.13.0"
  val ScalaVersions = Seq(Scala212, Scala213)

  val AkkaVersion = System.getProperty("override.akka.version", "2.5.23")
  val CassandraVersionInDocs = "4.0"
  val DriverVersion = "4.3.0"

  val akkaCassandraSessionDependencies = Seq(
    "com.datastax.oss" % "java-driver-core" % DriverVersion,
    "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
    "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test)

  val akkaPersistenceCassandraDependencies = Seq(
    "com.datastax.oss" % "java-driver-core" % DriverVersion,
    // Specifying guava dependency because older transitive dependency has security vulnerability
    "com.google.guava" % "guava" % "27.0.1-jre",
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
    "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion % Test,
    "ch.qos.logback" % "logback-classic" % "1.2.3" % Test,
    "org.scalatest" %% "scalatest" % "3.0.8" % Test,
    "org.pegdown" % "pegdown" % "1.6.0" % Test,
    "org.osgi" % "org.osgi.core" % "5.0.0" % Provided)
}
