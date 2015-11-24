package akka.persistence.cassandra.journal

import scala.concurrent.duration._
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.journal._
import akka.persistence.cassandra.CassandraLifecycle

import com.typesafe.config.ConfigFactory

object CassandraJournalConfiguration {
  lazy val config = ConfigFactory.parseString(
    s"""
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
      |akka.test.single-expect-default = 10s
      |cassandra-journal.port = ${CassandraLauncher.randomPort}
      |cassandra-snapshot-store.port = ${CassandraLauncher.randomPort}
    """.stripMargin)
}

class CassandraJournalSpec extends JournalSpec(CassandraJournalConfiguration.config) with CassandraLifecycle {
  override def systemName: String = "CassandraJournalSpec"
}

class CassandraJournalPerfSpec extends JournalPerfSpec(CassandraJournalConfiguration.config) with CassandraLifecycle {
  override def systemName: String = "CassandraJournalPerfSpec"

  override def awaitDurationMillis: Long = 20.seconds.toMillis
}
