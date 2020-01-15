/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.Props
import akka.persistence.cassandra.journal.MultiPluginSpec._
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraPluginConfig, CassandraSpec }
import akka.persistence.{ PersistentActor, SaveSnapshotSuccess }
import com.typesafe.config.ConfigFactory

object MultiPluginSpec {
  val journalKeyspace = "multiplugin_spec_journal"
  val snapshotKeyspace = "multiplugin_spec_snapshot"
  val cassandraPort = CassandraLauncher.randomPort
  val config = ConfigFactory.parseString(s"""
       |akka.test.single-expect-default = 20s
       |akka.test.filter-leeway = 20s
       |
       |cassandra-journal.keyspace = $journalKeyspace
       |cassandra-journal.keyspace-autocreate=false
       |cassandra-journal.circuit-breaker.call-timeout = 30s
       |cassandra-snapshot-store.keyspace=$snapshotKeyspace
       |cassandra-snapshot-store.keyspace-autocreate=false
       |
       |cassandra-journal-a=$${cassandra-journal}
       |cassandra-journal-a.write.table=processor_a_messages
       |
       |cassandra-journal-b=$${cassandra-journal}
       |cassandra-journal-b.write.table=processor_b_messages
       |
       |cassandra-journal-c=$${cassandra-journal}
       |cassandra-journal-c.write.table=processor_c_messages
       |cassandra-snapshot-c=$${cassandra-snapshot-store}
       |cassandra-snapshot-c.table=snapshot_c_messages
       |
       |cassandra-journal-d=$${cassandra-journal}
       |cassandra-journal-d.write.table=processor_d_messages
       |cassandra-snapshot-d=$${cassandra-snapshot-store}
       |cassandra-snapshot-d.table=snapshot_d_messages
       |
    """.stripMargin)

  trait Processor extends PersistentActor {

    override def receiveRecover: Receive = {
      case _ =>
    }

    override def receiveCommand: Receive = {
      case _: SaveSnapshotSuccess =>
      case "snapshot"             => saveSnapshot("snapshot")
      case payload =>
        persist(payload) { payload =>
          sender() ! s"$payload-$lastSequenceNr"
        }
    }

    def noop: Receive = {
      case _: String =>
    }
  }

  class OverrideJournalPluginProcessor(override val journalPluginId: String) extends Processor {
    override val persistenceId: String = "always-the-same"
    override val snapshotPluginId: String = "cassandra-snapshot-store"
  }

  class OverrideSnapshotPluginProcessor(override val journalPluginId: String, override val snapshotPluginId: String)
      extends Processor {
    override val persistenceId: String = "always-the-same"
  }

}

class MultiPluginSpec
    extends CassandraSpec(
      config.withFallback(ConfigFactory.load("reference.conf")),
      MultiPluginSpec.journalKeyspace,
      MultiPluginSpec.snapshotKeyspace) {

  lazy val cassandraPluginConfig =
    new CassandraPluginConfig(system, system.settings.config.getConfig("cassandra-journal"))

  // default journal plugin is not configured for this test
  override def awaitPersistenceInit(): Unit = ()

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    cluster.execute(
      s"CREATE KEYSPACE IF NOT EXISTS $journalKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    cluster.execute(
      s"CREATE KEYSPACE IF NOT EXISTS $snapshotKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

    CassandraLifecycle.awaitPersistenceInit(system, "cassandra-journal-a.write")
    CassandraLifecycle.awaitPersistenceInit(system, "cassandra-journal-b.write")
    CassandraLifecycle.awaitPersistenceInit(system, "cassandra-journal-c.write", "cassandra-snapshot-c")
    CassandraLifecycle.awaitPersistenceInit(system, "cassandra-journal-d.write", "cassandra-snapshot-d")
  }

  "A Cassandra journal" must {
    "be usable multiple times with different configurations for two actors having the same persistence id" in {
      val processorA = system.actorOf(Props(classOf[OverrideJournalPluginProcessor], "cassandra-journal-a.write"))
      processorA ! s"msg"
      expectMsgAllOf(s"msg-1")

      val processorB = system.actorOf(Props(classOf[OverrideJournalPluginProcessor], "cassandra-journal-b.write"))
      processorB ! s"msg"
      expectMsgAllOf(s"msg-1")

      processorB ! s"msg"
      expectMsgAllOf(s"msg-2")

      // c is actually a and therefore the next message must be seqNr 2 and not 3
      val processorC = system.actorOf(Props(classOf[OverrideJournalPluginProcessor], "cassandra-journal-a.write"))
      processorC ! s"msg"
      expectMsgAllOf(s"msg-2")
    }
  }

  "A Cassandra snapshot store" must {
    "be usable multiple times with different configurations for two actors having the same persistence id" in {
      val processorC =
        system.actorOf(
          Props(classOf[OverrideSnapshotPluginProcessor], "cassandra-journal-c.write", "cassandra-snapshot-c"))
      processorC ! s"msg"
      expectMsgAllOf(s"msg-1")

      val processorD =
        system.actorOf(
          Props(classOf[OverrideSnapshotPluginProcessor], "cassandra-journal-d.write", "cassandra-snapshot-d"))
      processorD ! s"msg"
      expectMsgAllOf(s"msg-1")

      processorD ! s"msg"
      expectMsgAllOf(s"msg-2")

      processorC ! "snapshot"
      processorD ! "snapshot"

      // e is actually c and therefore the next message must be seqNr 2 after recovery by using the snapshot
      val processorE =
        system.actorOf(
          Props(classOf[OverrideSnapshotPluginProcessor], "cassandra-journal-c.write", "cassandra-snapshot-c"))
      processorE ! s"msg"
      expectMsgAllOf(s"msg-2")

      // e is actually c and therefore the next message must be seqNr 2 after recovery by using the snapshot
      val processorF =
        system.actorOf(
          Props(classOf[OverrideSnapshotPluginProcessor], "cassandra-journal-d.write", "cassandra-snapshot-d"))
      processorF ! s"msg"
      expectMsgAllOf(s"msg-3")

    }
  }
}
