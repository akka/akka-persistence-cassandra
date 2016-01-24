/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
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
      |akka.test.single-expect-default = 20s
      |cassandra-journal.port = ${CassandraLauncher.randomPort}
      |cassandra-snapshot-store.port = ${CassandraLauncher.randomPort}
      |cassandra-journal.circuit-breaker.call-timeout = 20s
    """.stripMargin
  )

  lazy val perfConfig = ConfigFactory.parseString(
    """
    akka.actor.serialize-messages=off
    """
  ).withFallback(config)
}

class CassandraJournalSpec extends JournalSpec(CassandraJournalConfiguration.config) with CassandraLifecycle {
  override def systemName: String = "CassandraJournalSpec"

  override def supportsRejectingNonSerializableObjects = false
}

class CassandraJournalPerfSpec extends JournalPerfSpec(CassandraJournalConfiguration.perfConfig) with CassandraLifecycle {
  override def systemName: String = "CassandraJournalPerfSpec"

  override def awaitDurationMillis: Long = 20.seconds.toMillis

  override def supportsRejectingNonSerializableObjects = false
}
