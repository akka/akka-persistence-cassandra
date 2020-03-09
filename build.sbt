ThisBuild / resolvers += "Akka Snapshots".at("https://repo.akka.io/snapshots/")

lazy val root = (project in file("."))
  .enablePlugins(Common, ScalaUnidocPlugin)
  .disablePlugins(SitePlugin)
  .aggregate(core, cassandraLauncher, session)
  .settings(name := "akka-persistence-cassandra-root", publish / skip := true)

lazy val session = (project in file("session"))
  .enablePlugins(Common, AutomateHeaderPlugin)
  .dependsOn(cassandraLauncher % Test)
  .settings(
    name := "akka-cassandra-session",
    libraryDependencies ++= Dependencies.akkaCassandraSessionDependencies,
    Compile / packageBin / packageOptions += Package.ManifestAttributes(
        "Automatic-Module-Name" -> "akka.stream.alpakka.cassandra"))

lazy val dumpSchema = taskKey[Unit]("Dumps cassandra schema for docs")
dumpSchema := (core / runMain in (Test)).toTask(" akka.persistence.cassandra.PrintCreateStatements").value

lazy val core = (project in file("core"))
  .enablePlugins(Common, AutomateHeaderPlugin, MultiJvmPlugin)
  .dependsOn(cassandraLauncher % Test, session)
  .settings(
    name := "akka-persistence-cassandra",
    libraryDependencies ++= Dependencies.akkaPersistenceCassandraDependencies ++ Dependencies.silencer,
    Compile / packageBin / packageOptions += Package.ManifestAttributes(
        "Automatic-Module-Name" -> "akka.persistence.cassandra"))
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

lazy val docs = project
  .enablePlugins(Common, AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .dependsOn(core)
  .settings(
    name := "Akka Persistence Cassandra",
    (Compile / paradox) := (Compile / paradox).dependsOn(root / dumpSchema).value,
    publish / skip := true,
    whitesourceIgnore := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/akka-persistence-cassandra/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Paradox / siteSubdirName := s"docs/akka-persistence-cassandra/${projectInfoVersion.value}",
    paradoxProperties ++= Map(
        "akka.version" -> Dependencies.AkkaVersion,
        // Akka
        "extref.akka.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersionInDocs}/%s",
        "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AkkaVersionInDocs}/",
        "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/${Dependencies.AkkaVersionInDocs}/",
        // Alpakka
        "extref.alpakka.base_url" -> s"https://doc.akka.io/docs/alpakka/${Dependencies.AlpakkaVersionInDocs}/%s",
        // TODO switch this
        // "scaladoc.akka.stream.alpakka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AlpakkaVersionInDocs}/",
        "scaladoc.akka.stream.alpakka.base_url" -> s"/${(Preprocess / siteSubdirName).value}/",
        "javadoc.akka.stream.alpakka.base_url" -> "",
        // Cassandra
        "extref.cassandra.base_url" -> s"https://cassandra.apache.org/doc/${Dependencies.CassandraVersionInDocs}/%s",
        // Datastax Java driver
        "extref.java-driver.base_url" -> s"https://docs.datastax.com/en/developer/java-driver/${Dependencies.DriverVersionInDocs}/%s",
        "javadoc.com.datastax.oss.base_url" -> s"https://docs.datastax.com/en/drivers/java/${Dependencies.DriverVersionInDocs}/",
        // Java
        "javadoc.base_url" -> "https://docs.oracle.com/javase/8/docs/api/",
        // Scala
        "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/${scalaBinaryVersion.value}.x/",
        "scaladoc.akka.persistence.cassandra.base_url" -> s"/${(Preprocess / siteSubdirName).value}/",
        "javadoc.akka.persistence.cassandra.base_url" -> ""), // no Javadoc is published
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    ApidocPlugin.autoImport.apidocRootPackage := "akka",
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifact := makeSite.value -> "www/",
    publishRsyncHost := "akkarepo@gustav.akka.io",
    apidocRootPackage := "akka")

TaskKey[Unit]("verifyCodeFmt") := {
  scalafmtCheckAll.all(ScopeFilter(inAnyProject)).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted Scala code found. Please run 'scalafmtAll' and commit the reformatted code")
  }
  (Compile / scalafmtSbtCheck).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted sbt code found. Please run 'scalafmtSbt' and commit the reformatted code")
  }
}

addCommandAlias("verifyCodeStyle", "headerCheckAll; verifyCodeFmt")
