/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.snapshot

import java.lang.{ Long => JLong }
import java.lang.{ Integer => JInteger }
import java.nio.ByteBuffer

import akka.persistence.SnapshotProtocol._
import akka.persistence._
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.snapshot.SnapshotStoreSpec
import akka.stream.alpakka.cassandra.CassandraMetricsRegistry
import akka.testkit.TestProbe
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.typesafe.config.ConfigFactory

import scala.collection.immutable.Seq

object CassandraSnapshotStoreConfiguration {
  lazy val config = ConfigFactory.parseString(s"""
       akka.persistence.cassandra.journal.keyspace=CassandraSnapshotStoreSpec
       akka.persistence.cassandra.snapshot.keyspace=CassandraSnapshotStoreSpecSnapshot
       datastax-java-driver {
         basic.session-name = CassandraSnapshotStoreSpec
         advanced.metrics {
           session.enabled = [ "bytes-sent", "cql-requests"]
         }
       }
    """).withFallback(CassandraLifecycle.config)
}

class CassandraSnapshotStoreSpec
    extends SnapshotStoreSpec(CassandraSnapshotStoreConfiguration.config)
    with CassandraLifecycle {

  protected override def supportsMetadata: CapabilityFlag = true

  val snapshotSettings =
    new SnapshotSettings(system, system.settings.config.getConfig("akka.persistence.cassandra"))

  val storeStatements = new CassandraSnapshotStatements(snapshotSettings)

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
      val metricsNames = registry.getNames.toArray.toSet
      // metrics category is the configPath of the plugin + the session-name
      metricsNames should contain("akka.persistence.cassandra.CassandraSnapshotStoreSpec.bytes-sent")
      metricsNames should contain("akka.persistence.cassandra.CassandraSnapshotStoreSpec.cql-requests")
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
      snapshotStore.tell(SaveSnapshot(SnapshotMetadata(pid, 100).withMetadata("meta"), "snap"), probe.ref)
      probe.expectMsgType[SaveSnapshotSuccess]

      // load most recent snapshot
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)
      // get most recent snapshot
      val loaded = probe.expectMsgPF() { case LoadSnapshotResult(Some(snapshot), _) => snapshot }
      loaded.metadata.metadata should equal(Some("meta"))
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
