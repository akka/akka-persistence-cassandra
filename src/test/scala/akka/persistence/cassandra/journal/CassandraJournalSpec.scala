package akka.persistence.cassandra.journal

import akka.persistence.cassandra.CassandraCleanup
import akka.persistence.journal.LegacyJournalSpec

import com.typesafe.config.ConfigFactory

class CassandraJournalSpec extends LegacyJournalSpec with CassandraCleanup {
  lazy val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
    """.stripMargin)
}