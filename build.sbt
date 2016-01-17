import Tests._
import de.heikoseeberger.sbtheader.HeaderPattern
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

enablePlugins(AutomateHeaderPlugin)

organization := "com.github.krasserm"

name := "akka-persistence-cassandra"

version := "0.7-SNAPSHOT"

scalaVersion := "2.11.7"

fork in Test := true

javaOptions in Test += "-Xmx2500M"

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-deprecation",
  //"-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Xfuture"
)

parallelExecution in Test := false

val AkkaVersion = "2.4.1"

libraryDependencies ++= Seq(
  "com.datastax.cassandra"  % "cassandra-driver-core"             % "2.1.5",
  "com.typesafe.akka"      %% "akka-persistence"                  % AkkaVersion,
  "com.typesafe.akka"      %% "akka-persistence-tck"              % AkkaVersion      % "test",
  "org.scalatest"          %% "scalatest"                         % "2.1.4"      % "test",
  "org.cassandraunit"       % "cassandra-unit"                    % "2.1.9.2"    % "test"
)

credentials += Credentials(
  "Artifactory Realm",
  "oss.jfrog.org",
  sys.env.getOrElse("OSS_JFROG_USER", ""),
  sys.env.getOrElse("OSS_JFROG_PASS", "")
)

publishTo := {
  val jfrog = "https://oss.jfrog.org/artifactory/"
  if (isSnapshot.value)
    Some("OJO Snapshots" at jfrog + "oss-snapshot-local")
  else
    Some("OJO Releases" at jfrog + "oss-release-local")
}

publishMavenStyle := true

headers := headers.value ++ Map(
  "scala" -> (
    HeaderPattern.cStyleBlockComment,
    """|/*
       | * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
       | */
       |""".stripMargin
  )
)

SbtScalariform.scalariformSettings
ScalariformKeys.preferences in Compile  := formattingPreferences
ScalariformKeys.preferences in Test     := formattingPreferences

def formattingPreferences = {
  import scalariform.formatter.preferences._
  FormattingPreferences()
    .setPreference(RewriteArrowSymbols, false)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(SpacesAroundMultiImports, true)
}
