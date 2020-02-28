import sbt._
import Keys._

object Dependencies {
  val Scala212 = "2.12.10"
  val Scala213 = "2.13.1"
  val ScalaVersions = Seq(Scala212, Scala213)

  val AkkaVersion = System.getProperty("override.akka.version", "2.6.3")
  val AkkaVersionInDocs = System.getProperty("override.akka.version", "2.6")
  val CassandraVersionInDocs = "4.0"
  val DriverVersion = "4.3.0"
  val DriverVersionInDocs = "4.3"

  val AlpakkaVersion = "2.0.0-M3"
  val AlpakkaVersionInDocs = "2.0"

  val Logback = "ch.qos.logback" % "logback-classic" % "1.2.3"

  val silencerVersion = "1.4.4"
  val silencer = Seq(
    compilerPlugin(("com.github.ghik" %% "silencer-plugin" % silencerVersion).cross(CrossVersion.patch)),
    ("com.github.ghik" %% "silencer-lib" % silencerVersion % Provided).cross(CrossVersion.patch))

  val akkaCassandraSessionDependencies = Seq(
    ("com.datastax.oss" % "java-driver-core" % DriverVersion).exclude("com.github.spotbugs", "spotbugs-annotations"),
    "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
    "com.typesafe.akka" %% "akka-discovery" % AkkaVersion % Provided,
    "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion % Test,
    "org.scalatest" %% "scalatest" % "3.1.0" % Test,
    "com.novocode" % "junit-interface" % "0.11" % Test,
    "junit" % "junit" % "4.13" % Test,
    Logback % Test)

  val reconcilerDependencies = Seq(
    ("com.datastax.oss" % "java-driver-core" % DriverVersion).exclude("com.github.spotbugs", "spotbugs-annotations"),
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
      ("com.datastax.oss" % "java-driver-core" % DriverVersion).exclude("com.github.spotbugs", "spotbugs-annotations"),
      // Specifying guava dependency because older transitive dependency has security vulnerability
      "com.google.guava" % "guava" % "27.0.1-jre",
      "com.typesafe.akka" %% "akka-persistence" % AkkaVersion,
      "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion,
      "com.typesafe.akka" %% "akka-cluster-tools" % AkkaVersion,
      Logback % Test,
      "org.scalatest" %% "scalatest" % "3.1.0" % Test,
      "org.pegdown" % "pegdown" % "1.6.0" % Test,
      "org.osgi" % "org.osgi.core" % "5.0.0" % Provided) ++ akkaTestDeps.map(_ % AkkaVersion % Test)

  val dseTestDependencies = Seq(
    "com.datastax.dse" % "dse-java-driver-core" % "2.3.0" % Test,
    "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
    "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
    Logback % Test)
}
