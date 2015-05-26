package akka.persistence.cassandra.journal

import akka.persistence.journal._
import akka.persistence.cassandra.CassandraLifecycle

import com.typesafe.config.ConfigFactory

object CassandraJournalConfiguration {
  lazy val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
      |akka.test.single-expect-default = 10s
      |cassandra-journal.port = 9142
      |cassandra-snapshot-store.port = 9142
    """.stripMargin)
}

class CassandraJournalSpec extends JournalSpec(CassandraJournalConfiguration.config) with CassandraLifecycle

class CassandraJournalPerfSpec extends JournalPerfSpec(CassandraJournalConfiguration.config) with CassandraLifecycle
