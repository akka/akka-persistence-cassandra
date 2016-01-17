/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import scala.concurrent.duration._
import akka.persistence.journal._
import akka.persistence.cassandra.CassandraLifecycle

import com.typesafe.config.ConfigFactory

object CassandraJournalConfiguration {
  lazy val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
      |akka.test.single-expect-default = 20s
      |cassandra-journal.port = 9142
      |cassandra-snapshot-store.port = 9142
      |cassandra-journal.circuit-breaker.call-timeout = 20s
    """.stripMargin
  )
}

class CassandraJournalSpec extends JournalSpec(CassandraJournalConfiguration.config) with CassandraLifecycle {
  override def supportsRejectingNonSerializableObjects = false
}

class CassandraJournalPerfSpec extends JournalPerfSpec(CassandraJournalConfiguration.config) with CassandraLifecycle {

  override def awaitDurationMillis: Long = 20.seconds.toMillis

  override def supportsRejectingNonSerializableObjects = false
}
