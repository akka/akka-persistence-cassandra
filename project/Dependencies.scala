import sbt._
import Keys._

object Dependencies {
  val Scala213 = "2.13.10"
  val Scala3 = "3.3.1"
  val Scala2Versions = Seq(Scala213)
  val ScalaVersions = Dependencies.Scala2Versions :+ Dependencies.Scala3

  val AkkaVersion = System.getProperty("override.akka.version", "2.7.0")
  val AkkaVersionInDocs = AkkaVersion.take(3)
  val CassandraVersionInDocs = "4.0"
  // Should be sync with the version of the driver in Alpakka Cassandra
  val CassandraDriverVersion = "4.17.0"
  val DriverVersionInDocs = "4.14"

  val AlpakkaVersion = "6.0.0"
  val AlpakkaVersionInDocs = AlpakkaVersion
  // for example
  val AkkaManagementVersion = "1.2.0"

  val Logback = "ch.qos.logback" % "logback-classic" % "1.2.12"

  val reconcilerDependencies = Seq(
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test)

  val akkaTestDeps = Seq(
    "com.typesafe.akka" %% "akka-persistence",
    "com.typesafe.akka" %% "akka-persistence-typed",
    "com.typesafe.akka" %% "akka-persistence-query",
    "com.typesafe.akka" %% "akka-cluster-typed",
    "com.typesafe.akka" %% "akka-actor-testkit-typed",
    "com.typesafe.akka" %% "akka-persistence-tck",
    "com.typesafe.akka" %% "akka-stream-testkit",
    "com.typesafe.akka" %% "akka-multi-node-testkit",
    "com.typesafe.akka" %% "akka-cluster-sharding")

  val akkaPersistenceCassandraDependencies = Seq(
      "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % AlpakkaVersion,
      "com.typesafe.akka" %% "akka-persistence" % AkkaVersion,
      "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion,
      "com.typesafe.akka" %% "akka-cluster-tools" % AkkaVersion,
      "com.datastax.oss" % "java-driver-core" % CassandraDriverVersion,
      Logback % Test,
      "org.scalatest" %% "scalatest" % "3.2.17" % Test) ++ akkaTestDeps.map(_ % AkkaVersion % Test)

  val exampleDependencies = Seq(
    Logback,
    "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion,
    "com.typesafe.akka" %% "akka-discovery" % AkkaVersion,
    "com.typesafe.akka" %% "akka-serialization-jackson" % AkkaVersion,
    "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion,
    "com.lightbend.akka.management" %% "akka-management" % AkkaManagementVersion,
    "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % AkkaManagementVersion,
    "com.lightbend.akka.management" %% "akka-management-cluster-http" % AkkaManagementVersion,
    "com.lightbend.akka.discovery" %% "akka-discovery-kubernetes-api" % AkkaManagementVersion,
    "org.hdrhistogram" % "HdrHistogram" % "2.1.12")

  val dseTestDependencies = Seq(
    "com.datastax.dse" % "dse-java-driver-core" % "2.3.0" % Test,
    "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
    Logback % Test)
}
