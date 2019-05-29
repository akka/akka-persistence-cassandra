import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

lazy val root = (project in file("."))
  .aggregate(core, cassandraLauncher)
  .settings(
    name := "akka-persistence-cassandra-root",
    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
    publish := {},
    PgpKeys.publishSigned := {})

lazy val alpakkaCassandra = project.in(file("session"))
  .enablePlugins(Common)
  .settings(
    name := "alpakka-cassandra",
    libraryDependencies ++= Dependencies.cassandraSession
  )

lazy val core = (project in file("core"))
  .enablePlugins(Common, SbtOsgi, MultiJvmPlugin)
  .dependsOn(alpakkaCassandra, cassandraLauncher % Test)
  .settings(osgiSettings: _*)
  .settings({
    val silencerVersion = "1.3.1"
    Seq(
      libraryDependencies ++= Seq(
        compilerPlugin("com.github.ghik" %% "silencer-plugin" % silencerVersion),
        "com.github.ghik" %% "silencer-lib" % silencerVersion % Provided),
      // Hack because 'provided' dependencies by default are not picked up by the multi-jvm plugin:
      managedClasspath in MultiJvm ++= (managedClasspath in Compile).value.filter(_.data.name.contains("silencer-lib"))
      )
  })
  .settings(
    name := "akka-persistence-cassandra",
    libraryDependencies ++= Dependencies.akkaPersistenceCassandraDependencies,
    OsgiKeys.exportPackage := Seq("akka.persistence.cassandra.*"),
    OsgiKeys.importPackage := Seq(akkaImport(), optionalImport("org.apache.cassandra.*"), "*"),
    OsgiKeys.privatePackage := Nil,
    testOptions in Test ++= Seq(
        Tests.Argument(TestFrameworks.ScalaTest, "-o"),
        Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports")))
  .configs(MultiJvm)

lazy val cassandraLauncher = (project in file("cassandra-launcher"))
  .enablePlugins(Common)
  .settings(
    name := "akka-persistence-cassandra-launcher",
    managedResourceDirectories in Compile += (target in cassandraBundle).value / "bundle",
    managedResources in Compile += (assembly in cassandraBundle).value)

// This project doesn't get published directly, rather the assembled artifact is included as part of cassandraLaunchers
// resources
lazy val cassandraBundle = (project in file("cassandra-bundle"))
  .enablePlugins(Common)
  .settings(
    name := "akka-persistence-cassandra-bundle",
    crossPaths := false,
    autoScalaLibrary := false,
    libraryDependencies += ("org.apache.cassandra" % "cassandra-all" % "3.11.3")
        .exclude("commons-logging", "commons-logging"),
    dependencyOverrides += "com.github.jbellis" % "jamm" % "0.3.3", // See jamm comment in https://issues.apache.org/jira/browse/CASSANDRA-9608
    target in assembly := target.value / "bundle" / "akka" / "persistence" / "cassandra" / "launcher",
    assemblyJarName in assembly := "cassandra-bundle.jar")

def akkaImport(packageName: String = "akka.*") =
  versionedImport(packageName, "2.4", "2.5")
def configImport(packageName: String = "com.typesafe.config.*") =
  versionedImport(packageName, "1.3.0", "1.4.0")
def versionedImport(packageName: String, lower: String, upper: String) =
  s"""$packageName;version="[$lower,$upper)""""
def optionalImport(packageName: String) = s"$packageName;resolution:=optional"
