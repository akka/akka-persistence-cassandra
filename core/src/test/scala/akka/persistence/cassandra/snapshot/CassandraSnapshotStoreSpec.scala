/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import scala.concurrent.duration._
import java.lang.{ Long => JLong }
import java.lang.{ Integer => JInteger }
import java.nio.ByteBuffer

import akka.persistence._
import akka.persistence.SnapshotProtocol._
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraMetricsRegistry }
import akka.persistence.snapshot.SnapshotStoreSpec
import akka.testkit.TestProbe
import com.datastax.driver.core._
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import akka.persistence.cassandra.SnapshotWithMetaData

import scala.util.Try

object CassandraSnapshotStoreConfiguration {
  lazy val config = ConfigFactory.parseString(
    s"""
       |cassandra-journal.keyspace=CassandraSnapshotStoreSpec
       |cassandra-snapshot-store.keyspace=CassandraSnapshotStoreSpecSnapshot
       |cassandra-snapshot-store.max-metadata-result-size = 2
    """.stripMargin).withFallback(CassandraLifecycle.config)

  lazy val protocolV3Config = ConfigFactory.parseString(
    s"""
      cassandra-journal.protocol-version = 3
      cassandra-journal.enable-events-by-tag-query = off
      cassandra-journal.keyspace=CassandraSnapshotStoreProtocolV3Spec
      cassandra-snapshot-store.keyspace=CassandraSnapshotStoreProtocolV3Spec
    """).withFallback(config)
}

class CassandraSnapshotStoreSpec extends SnapshotStoreSpec(CassandraSnapshotStoreConfiguration.config) with CassandraLifecycle {

  val storeConfig = new CassandraSnapshotStoreConfig(system, system.settings.config.getConfig("cassandra-snapshot-store"))
  val storeStatements = new CassandraStatements {
    def snapshotConfig = storeConfig
  }

  var session: Session = _

  import storeStatements._

  override def systemName: String = "CassandraSnapshotStoreSpec"

  override def beforeAll(): Unit = {
    super.beforeAll()
    import system.dispatcher
    session = Await.result(storeConfig.sessionProvider.connect(), 5.seconds)
  }

  override def afterAll(): Unit = {
    Try {
      session.close()
      session.getCluster.close()
    }
    super.afterAll()
  }

  // ByteArraySerializer
  val serId: JInteger = 4

  "A Cassandra snapshot store" must {
    "insert Cassandra metrics to Cassandra Metrics Registry" in {
      val registry = CassandraMetricsRegistry(system).getRegistry
      val snapshots = registry.getNames.toArray()
      snapshots.length should be > 0
    }
    "make up to 3 snapshot loading attempts" in {
      val probe = TestProbe()

      // load most recent snapshot
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)

      // get most recent snapshot
      val expected = probe.expectMsgPF() { case LoadSnapshotResult(Some(snapshot), _) => snapshot }

      // write two more snapshots that cannot be de-serialized.
      session.execute(writeSnapshot(withMeta = false), pid, 17L: JLong, 123L: JLong, serId, "", ByteBuffer.wrap("fail-1".getBytes("UTF-8")))
      session.execute(writeSnapshot(withMeta = false), pid, 18L: JLong, 124L: JLong, serId, "", ByteBuffer.wrap("fail-2".getBytes("UTF-8")))

      // load most recent snapshot, first two attempts will fail ...
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)

      // third attempt succeeds
      probe.expectMsg(LoadSnapshotResult(Some(expected), Long.MaxValue))
    }
    "give up after 3 snapshot loading attempts" in {
      val probe = TestProbe()

      // load most recent snapshot
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)

      // wait for most recent snapshot
      probe.expectMsgPF() { case LoadSnapshotResult(Some(snapshot), _) => snapshot }

      // write three more snapshots that cannot be de-serialized.
      session.execute(writeSnapshot(withMeta = false), pid, 17L: JLong, 123L: JLong, serId, "", ByteBuffer.wrap("fail-1".getBytes("UTF-8")))
      session.execute(writeSnapshot(withMeta = false), pid, 18L: JLong, 124L: JLong, serId, "", ByteBuffer.wrap("fail-2".getBytes("UTF-8")))
      session.execute(writeSnapshot(withMeta = false), pid, 19L: JLong, 125L: JLong, serId, "", ByteBuffer.wrap("fail-3".getBytes("UTF-8")))

      // load most recent snapshot, first three attempts will fail ...
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)

      // no 4th attempt has been made
      probe.expectMsgType[LoadSnapshotFailed]
    }

    "store and load additional meta" in {
      val probe = TestProbe()

      // Somewhat confusing that two things are called meta data, SnapshotMetadata and SnapshotWithMetaData.
      // However, user facing is only SnapshotWithMetaData, and we can't change SnapshotMetadata because that
      // is in akka-persistence
      snapshotStore.tell(SaveSnapshot(SnapshotMetadata(pid, 100), SnapshotWithMetaData("snap", "meta")), probe.ref)
      probe.expectMsgType[SaveSnapshotSuccess]

      // load most recent snapshot
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)
      // get most recent snapshot
      val loaded = probe.expectMsgPF() { case LoadSnapshotResult(Some(snapshot), _) => snapshot }
      loaded.snapshot should equal(SnapshotWithMetaData("snap", "meta"))
    }
  }

}

/**
 * Cassandra 2.2.0 or later should support protocol version V4, but as long as we
 * support 2.1.6+ we do some compatibility testing with V3.
 */
class CassandraSnapshotStoreProtocolV3Spec extends SnapshotStoreSpec(CassandraSnapshotStoreConfiguration.protocolV3Config)
  with CassandraLifecycle {

  override def systemName: String = "CassandraSnapshotStoreProtocolV3Spec"

}
