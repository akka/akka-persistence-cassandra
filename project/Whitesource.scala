import sbt._
import sbt.Keys._
import sbtwhitesource.WhiteSourcePlugin.autoImport._
import sbtwhitesource._
import com.typesafe.sbt.SbtGit.GitKeys._

object Whitesource extends AutoPlugin {
  override def requires = WhiteSourcePlugin

  override def trigger = allRequirements

  override lazy val projectSettings = Seq(
    // do not change the value of whitesourceProduct
    whitesourceProduct := "Lightbend Reactive Platform",
    whitesourceAggregateProjectName := {
       "akka-persistence-cassandra-" + (
         if (isSnapshot.value)
           if (gitCurrentBranch.value == "master") "master"
           else "adhoc"
         else majorMinor(version.value).getOrElse("snapshot"))
    },
    whitesourceAggregateProjectToken := {
      whitesourceAggregateProjectName.value match {
        case "akka-persistence-cassandra-0.54" => "856774a1-36f5-45f1-93f1-979129c939d5"
        case other if other.endsWith("-master") || other.endsWith("-adhoc") => other
        case other => throw new IllegalStateException("No integration configured for version '$other'")
      }
    }
  )

  def majorMinor(version: String): Option[String] =
    """\d+\.\d+""".r.findFirstIn(version)
}
