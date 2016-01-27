/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.snapshot

import akka.persistence.cassandra.testkit.CassandraLauncher
import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import akka.persistence._
import akka.persistence.SnapshotProtocol._
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.snapshot.SnapshotStoreSpec
import akka.testkit.TestProbe

import com.datastax.driver.core._
import com.typesafe.config.ConfigFactory

object CassandraSnapshotStoreConfiguration {
  lazy val config = ConfigFactory.parseString(
    s"""
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
      |akka.test.single-expect-default = 10s
      |cassandra-journal.port = ${CassandraLauncher.randomPort}
      |cassandra-snapshot-store.port = ${CassandraLauncher.randomPort}
      |cassandra-journal.keyspace=CassandraSnapshotStoreSpec
      |cassandra-snapshot-store.keyspace=CassandraSnapshotStoreSpecSnapshot
      |cassandra-snapshot-store.max-metadata-result-size = 2
    """.stripMargin
  )

  lazy val protocolV3Config = ConfigFactory.parseString(
    s"""
      cassandra-journal.protocol-version = 3
      cassandra-journal.keyspace=CassandraSnapshotStoreProtocolV3Spec
      cassandra-snapshot-store.keyspace=CassandraSnapshotStoreProtocolV3Spec
    """
  ).withFallback(config)
}

class CassandraSnapshotStoreSpec extends SnapshotStoreSpec(CassandraSnapshotStoreConfiguration.config) with CassandraLifecycle {

  val storeConfig = new CassandraSnapshotStoreConfig(system.settings.config.getConfig("cassandra-snapshot-store"))
  val storeStatements = new CassandraStatements { def config = storeConfig }

  var cluster: Cluster = _
  var session: Session = _

  import storeConfig._
  import storeStatements._

  override def systemName: String = "CassandraSnapshotStoreSpec"

  override def beforeAll(): Unit = {
    super.beforeAll()
    cluster = clusterBuilder.build()
    session = cluster.connect()
  }

  override def afterAll(): Unit = {
    session.close()
    cluster.close()
    super.afterAll()
  }

  "A Cassandra snapshot store" must {
    "make up to 3 snapshot loading attempts" in {
      val probe = TestProbe()

      // load most recent snapshot
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)

      // get most recent snapshot
      val expected = probe.expectMsgPF() { case LoadSnapshotResult(Some(snapshot), _) => snapshot }

      // write two more snapshots that cannot be de-serialized.
      session.execute(writeSnapshot, pid, 17L: JLong, 123L: JLong, ByteBuffer.wrap("fail-1".getBytes("UTF-8")))
      session.execute(writeSnapshot, pid, 18L: JLong, 124L: JLong, ByteBuffer.wrap("fail-2".getBytes("UTF-8")))

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
      session.execute(writeSnapshot, pid, 17L: JLong, 123L: JLong, ByteBuffer.wrap("fail-1".getBytes("UTF-8")))
      session.execute(writeSnapshot, pid, 18L: JLong, 124L: JLong, ByteBuffer.wrap("fail-2".getBytes("UTF-8")))
      session.execute(writeSnapshot, pid, 19L: JLong, 125L: JLong, ByteBuffer.wrap("fail-3".getBytes("UTF-8")))

      // load most recent snapshot, first three attempts will fail ...
      snapshotStore.tell(LoadSnapshot(pid, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)

      // no 4th attempt has been made
      probe.expectMsg(LoadSnapshotResult(None, Long.MaxValue))
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
