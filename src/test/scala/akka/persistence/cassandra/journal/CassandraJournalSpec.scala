/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import scala.concurrent.duration._
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.journal._
import akka.persistence.cassandra.{ CassandraMetricsRegistry, CassandraLifecycle }

import com.typesafe.config.ConfigFactory

object CassandraJournalConfiguration {
  lazy val config = ConfigFactory.parseString(
    s"""
      |cassandra-journal.keyspace=CassandraJournalSpec
      |cassandra-snapshot-store.keyspace=CassandraJournalSpecSnapshot
    """.stripMargin
  ).withFallback(CassandraLifecycle.config)

  lazy val perfConfig = ConfigFactory.parseString(
    """
    akka.actor.serialize-messages=off
    cassandra-journal.keyspace=CassandraJournalPerfSpec
    cassandra-snapshot-store.keyspace=CassandraJournalPerfSpecSnapshot
    """
  ).withFallback(config)

  lazy val protocolV3Config = ConfigFactory.parseString(
    s"""
      cassandra-journal.protocol-version = 3
      cassandra-journal.enable-events-by-tag-query = off
      cassandra-journal.keyspace=CassandraJournalProtocolV3Spec
      cassandra-snapshot-store.keyspace=CassandraJournalProtocolV3Spec
    """
  ).withFallback(config)

  lazy val compat2Config = ConfigFactory.parseString(
    s"""
      cassandra-journal.cassandra-2x-compat = on
      cassandra-journal.keyspace=CassandraJournalCompat2Spec
      cassandra-snapshot-store.keyspace=CassandraJournalCompat2Spec
    """
  ).withFallback(config)
}

class CassandraJournalSpec extends JournalSpec(CassandraJournalConfiguration.config) with CassandraLifecycle {
  override def systemName: String = "CassandraJournalSpec"

  override def supportsRejectingNonSerializableObjects = false
  "A Cassandra Journal" must {
    "insert Cassandra metrics to Cassandra Metrics Registry" in {
      val registry = CassandraMetricsRegistry(system).getRegistry
      val snapshots = registry.getNames.toArray()
      snapshots.length should be > 0

    }
  }
}

/**
 * Cassandra 2.2.0 or later should support protocol version V4, but as long as we
 * support 2.1.6+ we do some compatibility testing with V3.
 */
class CassandraJournalProtocolV3Spec extends JournalSpec(CassandraJournalConfiguration.protocolV3Config) with CassandraLifecycle {
  override def systemName: String = "CassandraJournalProtocolV3Spec"

  override def supportsRejectingNonSerializableObjects = false
}

class CassandraJournalCompat2Spec extends JournalSpec(CassandraJournalConfiguration.compat2Config) with CassandraLifecycle {
  override def systemName: String = "CassandraJournalCompat2Spec"

  override def supportsRejectingNonSerializableObjects = false
}

class CassandraJournalPerfSpec extends JournalPerfSpec(CassandraJournalConfiguration.perfConfig) with CassandraLifecycle {
  override def systemName: String = "CassandraJournalPerfSpec"

  override def awaitDurationMillis: Long = 20.seconds.toMillis

  override def supportsRejectingNonSerializableObjects = false
}
