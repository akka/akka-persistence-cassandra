/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.util.UUID

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec }
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest._

object CassandraIntegrationSpec {
  val config = ConfigFactory.parseString(
    s"""
      |akka.persistence.journal.max-deletion-batch-size = 3
      |akka.persistence.publish-confirmations = on
      |akka.persistence.publish-plugin-commands = on
      |cassandra-journal.target-partition-size = 5
      |cassandra-journal.max-result-size = 3
      |cassandra-journal.keyspace=CassandraIntegrationSpec
      |cassandra-snapshot-store.keyspace=CassandraIntegrationSpecSnapshot
    """.stripMargin).withFallback(CassandraLifecycle.config)

  case class DeleteTo(snr: Long)

  class ProcessorAtomic(val persistenceId: String, receiver: ActorRef) extends PersistentActor {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case DeleteTo(sequenceNr) =>
        deleteMessages(sequenceNr)
      case payload: List[_] =>
        persistAll(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        receiver ! payload
        receiver ! lastSequenceNr
        receiver ! recoveryRunning
    }
  }

  class ProcessorA(val persistenceId: String, receiver: ActorRef) extends PersistentActor {
    def receiveRecover: Receive = handle

    def receiveCommand: Receive = {
      case DeleteTo(sequenceNr) =>
        deleteMessages(sequenceNr)
      case payload: String =>
        persist(payload)(handle)
    }

    def handle: Receive = {
      case payload: String =>
        receiver ! payload
        receiver ! lastSequenceNr
        receiver ! recoveryRunning
    }
  }

  class ProcessorC(val persistenceId: String, probe: ActorRef) extends PersistentActor {
    var last: String = _

    def receiveRecover: Receive = {
      case SnapshotOffer(_, snapshot: String) =>
        last = snapshot
        probe ! s"offered-${last}"
      case payload: String =>
        handle(payload)
    }

    def receiveCommand: Receive = {
      case "snap" =>
        saveSnapshot(last)
      case SaveSnapshotSuccess(_) =>
        probe ! s"snapped-${last}"
      case payload: String =>
        persist(payload)(handle)
      case DeleteTo(sequenceNr) =>
        deleteMessages(sequenceNr)

    }

    def handle: Receive = {
      case payload: String =>
        last = s"${payload}-${lastSequenceNr}"
        probe ! s"updated-${last}"
    }
  }

  class ProcessorCNoRecover(override val persistenceId: String, probe: ActorRef, recoverConfig: Recovery) extends ProcessorC(persistenceId, probe) {
    override def recovery = recoverConfig
    override def preStart() = ()
  }

}

import akka.persistence.cassandra.journal.CassandraIntegrationSpec._

class CassandraIntegrationSpec extends CassandraSpec(config) with ImplicitSender with WordSpecLike with Matchers {

  def subscribeToRangeDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.DeleteMessagesTo])

  def awaitRangeDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.DeleteMessagesTo]

  def testRangeDelete(persistenceId: String): Unit = {
    val deleteProbe = TestProbe()
    subscribeToRangeDeletion(deleteProbe)

    val processor1 = system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    1L to 16L foreach { i =>
      processor1 ! s"a-${i}"
      expectMsgAllOf(s"a-${i}", i, false)
    }

    processor1 ! DeleteTo(3L)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    4L to 16L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }

    processor1 ! DeleteTo(7L)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], persistenceId, self))
    8L to 16L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }
  }

  "A Cassandra journal" should {
    "write and replay messages" in {
      val persistenceId = UUID.randomUUID().toString
      val processor1 = system.actorOf(Props(classOf[ProcessorA], persistenceId, self), "p1")
      1L to 16L foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorA], persistenceId, self), "p2")
      1L to 16L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, true)
      }

      processor2 ! "b"
      expectMsgAllOf("b", 17L, false)
    }

    "not replay range-deleted messages" in {
      val persistenceId = UUID.randomUUID().toString
      testRangeDelete(persistenceId)
    }

    "write and replay with persistAll greater than partition size skipping whole partition" in {
      val persistenceId = UUID.randomUUID().toString
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      processorAtomic ! List("a-1", "a-2", "a-3", "a-4", "a-5", "a-6")
      1L to 6L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val testProbe = TestProbe()
      val processor2 = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, testProbe.ref))
      1L to 6L foreach { i =>
        testProbe.expectMsgAllOf(s"a-${i}", i, true)
      }
      processor2
    }

    "write and replay with persistAll greater than partition size skipping part of a partition" in {
      val persistenceId = UUID.randomUUID().toString
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      processorAtomic ! List("a-1", "a-2", "a-3")
      1L to 3L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      processorAtomic ! List("a-4", "a-5", "a-6")
      4L to 6L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val testProbe = TestProbe()
      val processor2 = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, testProbe.ref))
      1L to 6L foreach { i =>
        testProbe.expectMsgAllOf(s"a-${i}", i, true)
      }
      processor2
    }

    "write and replay with persistAll less than partition size" in {
      val persistenceId = UUID.randomUUID().toString
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      processorAtomic ! List("a-1", "a-2", "a-3", "a-4")
      1L to 4L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }

      system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))
      1L to 4L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, true)
      }
    }

    "not replay messages deleted from the +1 partition" in {
      val persistenceId = UUID.randomUUID().toString
      val deleteProbe = TestProbe()
      subscribeToRangeDeletion(deleteProbe)
      val processorAtomic = system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, self))

      processorAtomic ! List("a-1", "a-2", "a-3", "a-4", "a-5", "a-6")
      1L to 6L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, false)
      }
      processorAtomic ! DeleteTo(5L)
      awaitRangeDeletion(deleteProbe)

      val testProbe = TestProbe()
      system.actorOf(Props(classOf[ProcessorAtomic], persistenceId, testProbe.ref))
      testProbe.expectMsgAllOf(s"a-6", 6, true)
    }
  }

  "A processor" should {
    "recover from a snapshot with follow-up messages" in {
      val persistenceId = UUID.randomUUID().toString
      val processor1 = system.actorOf(Props(classOf[ProcessorC], persistenceId, testActor))
      processor1 ! "a"
      expectMsg("updated-a-1")
      processor1 ! "snap"
      expectMsg("snapped-a-1")
      processor1 ! "b"
      expectMsg("updated-b-2")

      system.actorOf(Props(classOf[ProcessorC], persistenceId, testActor))
      expectMsg("offered-a-1")
      expectMsg("updated-b-2")
    }
    "recover from a snapshot with follow-up messages and an upper bound" in {
      val persistenceId = UUID.randomUUID().toString
      val processor1 = system.actorOf(Props(classOf[ProcessorCNoRecover], persistenceId, testActor, Recovery()))
      processor1 ! "a"
      expectMsg("updated-a-1")
      processor1 ! "snap"
      expectMsg("snapped-a-1")
      2L to 7L foreach { i =>
        processor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorCNoRecover], persistenceId, testActor, Recovery(toSequenceNr = 3L)))
      expectMsg("offered-a-1")
      expectMsg("updated-a-2")
      expectMsg("updated-a-3")
      processor2 ! "d"
      expectMsg("updated-d-8")
    }
    "recover from a snapshot without follow-up messages inside a partition" in {
      val persistenceId = UUID.randomUUID().toString
      val processor1 = system.actorOf(Props(classOf[ProcessorC], persistenceId, testActor))
      processor1 ! "a"
      expectMsg("updated-a-1")
      processor1 ! "snap"
      expectMsg("snapped-a-1")

      val processor2 = system.actorOf(Props(classOf[ProcessorC], persistenceId, testActor))
      expectMsg("offered-a-1")
      processor2 ! "b"
      expectMsg("updated-b-2")
    }
    "recover from a snapshot without follow-up messages at a partition boundary (where next partition is invalid)" in {
      val persistenceId = UUID.randomUUID().toString
      val processor1 = system.actorOf(Props(classOf[ProcessorC], persistenceId, testActor))
      1L to 5L foreach { i =>
        processor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }
      processor1 ! "snap"
      expectMsg("snapped-a-5")

      val processor2 = system.actorOf(Props(classOf[ProcessorC], persistenceId, testActor))
      expectMsg("offered-a-5")
      processor2 ! "b"
      expectMsg("updated-b-6")
    }
    "recover from a snapshot without follow-up messages at a partition boundary (where next partition contains a permanently deleted message)" in {
      val persistenceId = UUID.randomUUID().toString
      val deleteProbe = TestProbe()
      subscribeToRangeDeletion(deleteProbe)

      val processor1 = system.actorOf(Props(classOf[ProcessorC], persistenceId, testActor))
      1L to 5L foreach { i =>
        processor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }
      processor1 ! "snap"
      expectMsg("snapped-a-5")

      processor1 ! "a"
      expectMsg("updated-a-6")

      processor1 ! DeleteTo(6L)
      awaitRangeDeletion(deleteProbe)

      val processor2 = system.actorOf(Props(classOf[ProcessorC], persistenceId, testActor))
      expectMsg("offered-a-5")
      processor2 ! "b"
      expectMsg("updated-b-7") // no longer re-using sequence numbers
    }
    "properly recover after all messages have been deleted" in {
      val persistenceId = UUID.randomUUID().toString
      val deleteProbe = TestProbe()
      subscribeToRangeDeletion(deleteProbe)

      val p = system.actorOf(Props(classOf[ProcessorA], persistenceId, self))

      p ! "a"
      expectMsgAllOf("a", 1L, false)

      p ! DeleteTo(1L)
      awaitRangeDeletion(deleteProbe)

      val r = system.actorOf(Props(classOf[ProcessorA], persistenceId, self))

      r ! "b"
      expectMsgAllOf("b", 2L, false) // no longer re-using sequence numbers
    }
  }
}
