package akka.persistence.journal.cassandra

import akka.persistence.journal.JournalSpec
import akka.testkit._

import com.typesafe.config.ConfigFactory

class CassandraJournalSpec extends TestKitBase with JournalSpec with CassandraCleanup {
  lazy val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.snapshot-store.local.dir = "target/snapshots"
    """.stripMargin)
}