lazy val root = (project in file("."))
  .enablePlugins(Common, ScalaUnidocPlugin)
  .disablePlugins(SitePlugin)
  .aggregate(core, cassandraLauncher, session)
  .settings(name := "akka-persistence-cassandra-root", publish / skip := true)

lazy val session = (project in file("session"))
  .enablePlugins(Common, AutomateHeaderPlugin, SbtOsgi)
  .dependsOn(cassandraLauncher % Test)
  .settings(osgiSettings: _*)
  .settings(
    name := "akka-cassandra-session",
    libraryDependencies ++= Dependencies.akkaCassandraSessionDependencies,
    Compile / packageBin / packageOptions += Package.ManifestAttributes(
        "Automatic-Module-Name" -> "akka.cassandra.session"),
    OsgiKeys.exportPackage := Seq("akka.cassandra.session.*"),
    OsgiKeys.importPackage := Seq(akkaImport(), optionalImport("org.apache.cassandra.*"), "*"),
    OsgiKeys.privatePackage := Nil)

lazy val core = (project in file("core"))
  .enablePlugins(Common, AutomateHeaderPlugin, SbtOsgi, MultiJvmPlugin)
  .dependsOn(cassandraLauncher % Test, session)
  .settings(osgiSettings: _*)
  .settings({
    val silencerVersion = "1.4.4"
    Seq(
      libraryDependencies ++= Seq(
          compilerPlugin(("com.github.ghik" %% "silencer-plugin" % silencerVersion).cross(CrossVersion.patch)),
          ("com.github.ghik" %% "silencer-lib" % silencerVersion % Provided).cross(CrossVersion.patch)))
  })
  .settings(
    name := "akka-persistence-cassandra",
    libraryDependencies ++= Dependencies.akkaPersistenceCassandraDependencies,
    Compile / packageBin / packageOptions += Package.ManifestAttributes(
        "Automatic-Module-Name" -> "akka.persistence.cassandra"),
    OsgiKeys.exportPackage := Seq("akka.persistence.cassandra.*"),
    OsgiKeys.importPackage := Seq(akkaImport(), optionalImport("org.apache.cassandra.*"), "*"),
    OsgiKeys.privatePackage := Nil,
    testOptions in Test ++= Seq(Tests.Argument(TestFrameworks.ScalaTest, "-o")))
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
  .enablePlugins(Common, AutomateHeaderPlugin)
  .settings(
    name := "akka-persistence-cassandra-bundle",
    crossPaths := false,
    autoScalaLibrary := false,
    libraryDependencies += ("org.apache.cassandra" % "cassandra-all" % "3.11.3")
        .exclude("commons-logging", "commons-logging"),
    dependencyOverrides += "com.github.jbellis" % "jamm" % "0.3.3", // See jamm comment in https://issues.apache.org/jira/browse/CASSANDRA-9608
    target in assembly := target.value / "bundle" / "akka" / "persistence" / "cassandra" / "launcher",
    assemblyJarName in assembly := "cassandra-bundle.jar")

lazy val dseTest =
  (project in file("dse-test"))
    .dependsOn(core % "test->test")
    .settings(libraryDependencies ++= Dependencies.dseTestDependencies)

lazy val akka26Tests = (project in file("akka-26-tests"))
  .dependsOn(core % "test->test")
  .settings(libraryDependencies ++= Dependencies.akka26TestDependencies)

lazy val docs = project
  .enablePlugins(Common, AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .settings(
    name := "Akka Persistence Cassandra",
    publish / skip := true,
    whitesourceIgnore := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/akka-persistence-cassandra/${if (isSnapshot.value) "snapshot"
      else version.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Preprocess / preprocessRules := Seq(("\\.java\\.scala".r, _ => ".java")),
    Paradox / siteSubdirName := s"docs/akka-persistence-cassandra/${if (isSnapshot.value) "snapshot" else version.value}",
    paradoxProperties ++= Map(
        "akka.version" -> Dependencies.AkkaVersion,
        // Akka
        "extref.akka.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersion}/%s",
        "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AkkaVersion}/",
        "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/${Dependencies.AkkaVersion}/",
        // Cassandra
        "extref.cassandra.base_url" -> s"https://cassandra.apache.org/doc/${Dependencies.CassandraVersionInDocs}/%s",
        // Java
        "javadoc.base_url" -> "https://docs.oracle.com/javase/8/docs/api/",
        // Scala
        "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/${scalaBinaryVersion.value}.x/",
        "scaladoc.akka.persistence.cassandra.base_url" -> s"/${(Preprocess / siteSubdirName).value}/"),
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifact := makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io")

def akkaImport(packageName: String = "akka.*") =
  versionedImport(packageName, "2.4", "2.5")
def configImport(packageName: String = "com.typesafe.config.*") =
  versionedImport(packageName, "1.3.0", "1.4.0")
def versionedImport(packageName: String, lower: String, upper: String) =
  s"""$packageName;version="[$lower,$upper)""""
def optionalImport(packageName: String) = s"$packageName;resolution:=optional"

ThisBuild / dynverSonatypeSnapshots := true
