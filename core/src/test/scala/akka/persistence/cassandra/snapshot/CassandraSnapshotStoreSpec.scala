/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import java.lang.{ Long => JLong }
import java.lang.{ Integer => JInteger }
import java.nio.ByteBuffer

import akka.cassandra.session.CassandraMetricsRegistry
import akka.persistence.SnapshotProtocol._
import akka.persistence._
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.SnapshotWithMetaData
import akka.persistence.snapshot.SnapshotStoreSpec
import akka.testkit.TestProbe
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.typesafe.config.ConfigFactory

import scala.collection.immutable.Seq

object CassandraSnapshotStoreConfiguration {
  lazy val config = ConfigFactory.parseString(s"""
       |cassandra-journal.keyspace=CassandraSnapshotStoreSpec
       |cassandra-snapshot-store.keyspace=CassandraSnapshotStoreSpecSnapshot
    """.stripMargin).withFallback(CassandraLifecycle.config)
}

class CassandraSnapshotStoreSpec
    extends SnapshotStoreSpec(CassandraSnapshotStoreConfiguration.config)
    with CassandraLifecycle {

  val storeConfig =
    new CassandraSnapshotStoreConfig(system, system.settings.config.getConfig("cassandra-snapshot-store"))
  val storeStatements = new CassandraStatements {
    def snapshotConfig = storeConfig
  }

  import storeStatements._

  override def systemName: String = "CassandraSnapshotStoreSpec"

  override def beforeAll(): Unit = {
    super.beforeAll()
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
      cluster.execute(
        SimpleStatement.newInstance(
          writeSnapshot(withMeta = false),
          pid,
          17L: JLong,
          123L: JLong,
          serId,
          "",
          ByteBuffer.wrap("fail-1".getBytes("UTF-8"))))

      cluster.execute(
        SimpleStatement.newInstance(
          writeSnapshot(withMeta = false),
          pid,
          18L: JLong,
          124L: JLong,
          serId,
          "",
          ByteBuffer.wrap("fail-2".getBytes("UTF-8"))))

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
      cluster.execute(
        SimpleStatement.newInstance(
          writeSnapshot(withMeta = false),
          pid,
          17L: JLong,
          123L: JLong,
          serId,
          "",
          ByteBuffer.wrap("fail-1".getBytes("UTF-8"))))
      cluster.execute(
        SimpleStatement.newInstance(
          writeSnapshot(withMeta = false),
          pid,
          18L: JLong,
          124L: JLong,
          serId,
          "",
          ByteBuffer.wrap("fail-2".getBytes("UTF-8"))))
      cluster.execute(
        SimpleStatement.newInstance(
          writeSnapshot(withMeta = false),
          pid,
          19L: JLong,
          125L: JLong,
          serId,
          "",
          ByteBuffer.wrap("fail-3".getBytes("UTF-8"))))

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

    "delete all snapshots matching upper sequence number and no timestamp bounds" in {
      val probe: TestProbe = TestProbe()
      val subProbe: TestProbe = TestProbe()
      val metadata: Seq[SnapshotMetadata] = writeSnapshots()
      val md = metadata(2)
      val criteria = SnapshotSelectionCriteria(md.sequenceNr)
      val cmd = DeleteSnapshots(pid, criteria)

      subscribe[DeleteSnapshots](subProbe.ref)
      snapshotStore.tell(cmd, probe.ref)
      subProbe.expectMsg(cmd)
      probe.expectMsg(DeleteSnapshotsSuccess(criteria))

      snapshotStore.tell(
        LoadSnapshot(pid, SnapshotSelectionCriteria(md.sequenceNr, md.timestamp), Long.MaxValue),
        probe.ref)
      probe.expectMsg(LoadSnapshotResult(None, Long.MaxValue))
      snapshotStore.tell(
        LoadSnapshot(pid, SnapshotSelectionCriteria(metadata(3).sequenceNr, metadata(3).timestamp), Long.MaxValue),
        probe.ref)
      probe.expectMsg(LoadSnapshotResult(Some(SelectedSnapshot(metadata(3), s"s-4")), Long.MaxValue))
    }
  }
}
