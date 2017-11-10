/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import scala.concurrent.duration._
import akka.actor.{ ActorSystem, Props }
import akka.persistence.{ SaveSnapshotSuccess, PersistentActor }
import akka.persistence.cassandra.{ CassandraPluginConfig, CassandraLifecycle }
import akka.testkit.{ ImplicitSender, TestKit }
import com.datastax.driver.core.{ Session, Cluster }
import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpecLike
import scala.collection.JavaConverters._
import akka.persistence.cassandra.testkit.CassandraLauncher

import akka.persistence.cassandra.journal.MultiPluginSpec._
import scala.concurrent.Await

object MultiPluginSpec {
  val journalKeyspace = "multiplugin_spec_journal"
  val snapshotKeyspace = "multiplugin_spec_snapshot"
  val cassandraPort = CassandraLauncher.randomPort
  val config = ConfigFactory.parseString(
    s"""
        |akka.test.single-expect-default = 20s
        |
        |cassandra-journal.keyspace = $journalKeyspace
        |cassandra-journal.port=$cassandraPort
        |cassandra-journal.keyspace-autocreate=false
        |cassandra-journal.circuit-breaker.call-timeout = 30s
        |cassandra-snapshot-store.keyspace=$snapshotKeyspace
        |cassandra-snapshot-store.port=$cassandraPort
        |cassandra-snapshot-store.keyspace-autocreate=false
        |
        |cassandra-journal-a=$${cassandra-journal}
        |cassandra-journal-a.table=processor_a_messages
        |
        |cassandra-journal-b=$${cassandra-journal}
        |cassandra-journal-b.table=processor_b_messages
        |
        |cassandra-journal-c=$${cassandra-journal}
        |cassandra-journal-c.table=processor_c_messages
        |cassandra-snapshot-c=$${cassandra-snapshot-store}
        |cassandra-snapshot-c.table=snapshot_c_messages
        |
        |cassandra-journal-d=$${cassandra-journal}
        |cassandra-journal-d.table=processor_d_messages
        |cassandra-snapshot-d=$${cassandra-snapshot-store}
        |cassandra-snapshot-d.table=snapshot_d_messages
        |
    """.stripMargin
  )

  trait Processor extends PersistentActor {

    override def receiveRecover: Receive = {
      case payload =>
    }

    override def receiveCommand: Receive = {
      case _: SaveSnapshotSuccess =>
      case "snapshot"             => saveSnapshot("snapshot")
      case payload => persist(payload) { payload =>
        sender() ! s"$payload-$lastSequenceNr"
      }
    }

    def noop: Receive = {
      case payload: String =>
    }
  }

  class OverrideJournalPluginProcessor(override val journalPluginId: String) extends Processor {
    override val persistenceId: String = "always-the-same"
    override val snapshotPluginId: String = "cassandra-snapshot-store"
  }

  class OverrideSnapshotPluginProcessor(override val journalPluginId: String, override val snapshotPluginId: String) extends Processor {
    override val persistenceId: String = "always-the-same"
  }

}

class MultiPluginSpec
  extends TestKit(ActorSystem("MultiPluginSpec", config.withFallback(ConfigFactory.load("reference.conf")).resolve()))
  with ImplicitSender
  with WordSpecLike
  with CassandraLifecycle {

  override def systemName: String = "MultiPluginSpec"

  val cassandraPluginConfig = new CassandraPluginConfig(system, system.settings.config.getConfig("cassandra-journal"))

  var session: Session = _

  // default journal plugin is not configured for this test
  override def awaitPersistenceInit(): Unit = ()

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    import system.dispatcher
    session = Await.result(cassandraPluginConfig.sessionProvider.connect(), 25.seconds)

    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $journalKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $snapshotKeyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

    CassandraLifecycle.awaitPersistenceInit(system, "cassandra-journal-a")
    CassandraLifecycle.awaitPersistenceInit(system, "cassandra-journal-b")
    CassandraLifecycle.awaitPersistenceInit(system, "cassandra-journal-c", "cassandra-snapshot-c")
    CassandraLifecycle.awaitPersistenceInit(system, "cassandra-journal-d", "cassandra-snapshot-d")
  }

  override protected def afterAll(): Unit = {
    session.close()
    session.getCluster.close()

    super.afterAll()
  }

  "A Cassandra journal" should {
    "be usable multiple times with different configurations for two actors having the same persistence id" in {
      val processorA = system.actorOf(Props(classOf[OverrideJournalPluginProcessor], "cassandra-journal-a"))
      processorA ! s"msg"
      expectMsgAllOf(s"msg-1")

      val processorB = system.actorOf(Props(classOf[OverrideJournalPluginProcessor], "cassandra-journal-b"))
      processorB ! s"msg"
      expectMsgAllOf(s"msg-1")

      processorB ! s"msg"
      expectMsgAllOf(s"msg-2")

      // c is actually a and therefore the next message must be seqNr 2 and not 3
      val processorC = system.actorOf(Props(classOf[OverrideJournalPluginProcessor], "cassandra-journal-a"))
      processorC ! s"msg"
      expectMsgAllOf(s"msg-2")

    }
  }

  "A Cassandra snapshot store" should {
    "be usable multiple times with different configurations for two actors having the same persistence id" in {
      val processorC = system.actorOf(Props(classOf[OverrideSnapshotPluginProcessor], "cassandra-journal-c", "cassandra-snapshot-c"))
      processorC ! s"msg"
      expectMsgAllOf(s"msg-1")

      val processorD = system.actorOf(Props(classOf[OverrideSnapshotPluginProcessor], "cassandra-journal-d", "cassandra-snapshot-d"))
      processorD ! s"msg"
      expectMsgAllOf(s"msg-1")

      processorD ! s"msg"
      expectMsgAllOf(s"msg-2")

      processorC ! "snapshot"
      processorD ! "snapshot"

      // e is actually c and therefore the next message must be seqNr 2 after recovery by using the snapshot
      val processorE = system.actorOf(Props(classOf[OverrideSnapshotPluginProcessor], "cassandra-journal-c", "cassandra-snapshot-c"))
      processorE ! s"msg"
      expectMsgAllOf(s"msg-2")

      // e is actually c and therefore the next message must be seqNr 2 after recovery by using the snapshot
      val processorF = system.actorOf(Props(classOf[OverrideSnapshotPluginProcessor], "cassandra-journal-d", "cassandra-snapshot-d"))
      processorF ! s"msg"
      expectMsgAllOf(s"msg-3")

    }
  }
}
