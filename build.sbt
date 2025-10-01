import com.typesafe.sbt.packager.docker._
import com.geirsson.CiReleasePlugin

ThisBuild / resolvers += "Akka library repository".at("https://repo.akka.io/maven/github_actions")
ThisBuild / resolvers ++= {
  if (System.getProperty("override.akka.version") != null)
    Seq("Akka library snapshot repository".at("https://repo.akka.io/snapshots"))
  else Seq.empty
}

// make version compatible with docker for publishing example project
ThisBuild / dynverSeparator := "-"
// append -SNAPSHOT to version when isSnapshot
ThisBuild / dynverSonatypeSnapshots := true

lazy val root = project
  .in(file("."))
  .enablePlugins(Common, ScalaUnidocPlugin)
  .disablePlugins(SitePlugin, CiReleasePlugin)
  .aggregate(core)
  .settings(name := "akka-persistence-cassandra-root", publish / skip := true)

lazy val dumpSchema = taskKey[Unit]("Dumps cassandra schema for docs")
dumpSchema := (core / Test / runMain).toTask(" akka.persistence.cassandra.PrintCreateStatements").value

lazy val core = project
  .in(file("core"))
  .enablePlugins(Common, AutomateHeaderPlugin)
  .disablePlugins(CiReleasePlugin)
  .settings(
    name := "akka-persistence-cassandra",
    libraryDependencies ++= Dependencies.akkaPersistenceCassandraDependencies,
    Compile / packageBin / packageOptions += Package.ManifestAttributes(
        "Automatic-Module-Name" -> "akka.persistence.cassandra"))
  .settings(Scala3.settings)

// Used for testing events by tag in various environments
lazy val endToEndExample = project
  .in(file("example"))
  .dependsOn(core)
  .settings(libraryDependencies ++= Dependencies.exampleDependencies, publish / skip := true)
  .settings(
    dockerBaseImage := "docker.io/library/eclipse-temurin:17.0.8.1_1-jre",
    dockerCommands :=
      dockerCommands.value.flatMap {
        case ExecCmd("ENTRYPOINT", args @ _*) => Seq(Cmd("ENTRYPOINT", args.mkString(" ")))
        case v                                => Seq(v)
      },
    dockerExposedPorts := Seq(8080, 8558, 2552),
    dockerUsername := Some("kubakka"),
    dockerUpdateLatest := true,
    // update if deploying to some where that can't see docker hu
    //dockerRepository := Some("some-registry"),
    dockerCommands ++= Seq(Cmd("USER", "root"), Cmd("RUN", "chgrp -R 0 . && chmod -R g=u .")),
    // Docker image is only for running in k8s
    Universal / javaOptions ++= Seq("-J-Dconfig.resource=kubernetes.conf"))
  .enablePlugins(DockerPlugin, JavaAppPackaging)
  .disablePlugins(CiReleasePlugin)

lazy val dseTest = project
  .in(file("dse-test"))
  .dependsOn(core % "test->test")
  .settings(libraryDependencies ++= Dependencies.dseTestDependencies)
  .disablePlugins(CiReleasePlugin)

lazy val docs = project
  .enablePlugins(Common, AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(CiReleasePlugin)
  .dependsOn(core)
  .settings(
    name := "Akka Persistence plugin for Cassandra",
    (Compile / paradox) := (Compile / paradox).dependsOn(root / dumpSchema).value,
    publish / skip := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/akka-persistence-cassandra/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Paradox / siteSubdirName := s"libraries/akka-persistence-cassandra/${projectInfoVersion.value}",
    Compile / paradoxProperties ++= Map(
        "project.url" -> "https://doc.akka.io/libraries/akka-persistence-cassandra/current/",
        "canonical.base_url" -> "https://doc.akka.io/libraries/akka-persistence-cassandra/current",
        "akka.version" -> Dependencies.AkkaVersion,
        // Akka
        "extref.akka.base_url" -> s"https://doc.akka.io/libraries/akka-core/${Dependencies.AkkaVersionInDocs}/%s",
        "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AkkaVersionInDocs}/",
        "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/${Dependencies.AkkaVersionInDocs}/",
        // Alpakka
        "extref.alpakka.base_url" -> s"https://doc.akka.io/docs/alpakka/${Dependencies.AlpakkaVersionInDocs}/%s",
        "scaladoc.akka.stream.alpakka.base_url" -> s"https://doc.akka.io/api/alpakka/${Dependencies.AlpakkaVersionInDocs}/",
        "javadoc.akka.stream.alpakka.base_url" -> "",
        // APC 0.x
        "extref.apc-0.x.base_url" -> s"https://doc.akka.io/libraries/akka-persistence-cassandra/0.103/%s",
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
    publishRsyncArtifacts += makeSite.value -> "www/",
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

val isJdk11orHigher: Boolean = {
  val result = VersionNumber(sys.props("java.specification.version")).matchesSemVer(SemanticSelector(">=11"))
  if (!result)
    throw new IllegalArgumentException("JDK 11 or higher is required")
  result
}
