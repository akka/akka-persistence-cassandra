package akka.persistence.journal.cassandra

import akka.actor._
import akka.persistence.journal.JournalSpec
import akka.testkit._

import com.typesafe.config.ConfigFactory

object CassandraJournalSpec {
  val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.snapshot-store.local.dir = "target/snapshots"
    """.stripMargin).withFallback(JournalSpec.config)
}

class CassandraJournalSpec extends TestKit(ActorSystem("test", CassandraJournalSpec.config)) with JournalSpec with CassandraCleanup