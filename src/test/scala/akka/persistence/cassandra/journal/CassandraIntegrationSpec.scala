package akka.persistence.cassandra.journal

import scala.concurrent.duration._

import akka.actor._
import akka.persistence._
import akka.persistence.cassandra.CassandraCleanup
import akka.testkit._

import com.typesafe.config.ConfigFactory

import org.scalatest._

object CassandraIntegrationSpec {
  val config = ConfigFactory.parseString(
    """
      |akka.persistence.snapshot-store.plugin = "cassandra-snapshot-store"
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.journal.max-deletion-batch-size = 3
      |akka.persistence.publish-confirmations = on
      |akka.persistence.publish-plugin-commands = on
      |cassandra-journal.max-partition-size = 5
      |cassandra-journal.max-result-size = 3
    """.stripMargin)

  case class Delete(snr: Long, permanent: Boolean)
  case class DeleteTo(snr: Long, permanent: Boolean)

  class ProcessorA(val persistenceId: String) extends PersistentActor {
    def receiveRecover: Receive = handle
    def receiveCommand: Receive = {
      case Delete(sequenceNr, permanent) =>
        deleteMessage(sequenceNr, permanent)
      case DeleteTo(sequenceNr, permanent) =>
        deleteMessages(sequenceNr, permanent)
      case payload: String =>
        persist(payload)(handle)
    }
    def handle: Receive = {
      case payload: String =>
        sender ! payload
        sender ! lastSequenceNr
        sender ! recoveryRunning
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
      case Delete(sequenceNr, permanent) =>
        deleteMessage(sequenceNr, permanent)
      case payload: String =>
        persist(payload)(handle)

    }

    def handle: Receive = {
      case payload: String =>
        last = s"${payload}-${lastSequenceNr}"
        probe ! s"updated-${last}"
    }
  }

  class ProcessorCNoRecover(override val persistenceId: String, probe: ActorRef) extends ProcessorC(persistenceId, probe) {
    override def preStart() = ()
  }

  class ViewA(val viewId: String, val persistenceId: String, probe: ActorRef) extends PersistentView {
    def receive = {
      case payload =>
        probe ! payload
    }

    override def autoUpdate: Boolean = false
    override def autoUpdateReplayMax: Long = 0
  }
}

import CassandraIntegrationSpec._

class CassandraIntegrationSpec extends TestKit(ActorSystem("test", config)) with ImplicitSender with WordSpecLike with Matchers with CassandraCleanup {
  def subscribeToConfirmation(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[Delivered])

  def subscribeToBatchDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.DeleteMessages])

  def subscribeToRangeDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.DeleteMessagesTo])

  def awaitBatchDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.DeleteMessages]

  def awaitRangeDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.DeleteMessagesTo]

  def testIndividualDelete(processorId: String, permanent: Boolean) {
    val deleteProbe = TestProbe()
    subscribeToBatchDeletion(deleteProbe)

    val processor1 = system.actorOf(Props(classOf[ProcessorA], processorId))
    1L to 16L foreach { i =>
      processor1 ! s"a-${i}"
      expectMsgAllOf(s"a-${i}", i, false)
    }

    // delete single message in partition
    processor1 ! Delete(12L, permanent)
    awaitBatchDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], processorId))
    1L to 16L foreach { i =>
      if (i != 12L) expectMsgAllOf(s"a-${i}", i, true)
    }

    // delete whole partition
    6L to 10L foreach { i =>
      processor1 ! Delete(i, permanent)
      awaitBatchDeletion(deleteProbe)
    }

    system.actorOf(Props(classOf[ProcessorA], processorId))
    1L to 5L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }
    11L to 16L foreach { i =>
      if (i != 12L) expectMsgAllOf(s"a-${i}", i, true)
    }
  }

  def testRangeDelete(processorId: String, permanent: Boolean) {
    val deleteProbe = TestProbe()
    subscribeToRangeDeletion(deleteProbe)

    val processor1 = system.actorOf(Props(classOf[ProcessorA], processorId))
    1L to 16L foreach { i =>
      processor1 ! s"a-${i}"
      expectMsgAllOf(s"a-${i}", i, false)
    }

    processor1 ! DeleteTo(3L, permanent)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], processorId))
    4L to 16L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }

    processor1 ! DeleteTo(7L, permanent)
    awaitRangeDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], processorId))
    8L to 16L foreach { i =>
      expectMsgAllOf(s"a-${i}", i, true)
    }
  }

  "A Cassandra journal" should {
    "write and replay messages" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p1"))
      1L to 16L foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorA], "p1"))
      1L to 16L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, true)
      }

      processor2 ! "b"
      expectMsgAllOf("b", 17L, false)
    }
    "not replay messages marked as deleted" in {
      testIndividualDelete("p3", false)
    }
    "not replay permanently deleted messages" in {
      testIndividualDelete("p4", true)
    }
    "not replay messages marked as range-deleted" in {
      testRangeDelete("p5", false)
    }
    "not replay permanently range-deleted messages" in {
      testRangeDelete("p6", true)
    }
    "replay messages incrementally" in {
      val probe = TestProbe()
      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p7"))
      1L to 6L foreach { i =>
        processor1 ! s"a-${i}"
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val view = system.actorOf(Props(classOf[ViewA], "p7-view", "p7", probe.ref))
      probe.expectNoMsg(200.millis)

      view ! Update(true, replayMax = 3L)
      probe.expectMsg(s"a-1")
      probe.expectMsg(s"a-2")
      probe.expectMsg(s"a-3")
      probe.expectNoMsg(200.millis)

      view ! Update(true, replayMax = 3L)
      probe.expectMsg(s"a-4")
      probe.expectMsg(s"a-5")
      probe.expectMsg(s"a-6")
      probe.expectNoMsg(200.millis)
    }
  }

  "A processor" should {
    "recover from a snapshot with follow-up messages" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p10", testActor))
      processor1 ! "a"
      expectMsg("updated-a-1")
      processor1 ! "snap"
      expectMsg("snapped-a-1")
      processor1 ! "b"
      expectMsg("updated-b-2")

      system.actorOf(Props(classOf[ProcessorC], "p10", testActor))
      expectMsg("offered-a-1")
      expectMsg("updated-b-2")
    }
    "recover from a snapshot with follow-up messages and an upper bound" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorCNoRecover], "p11", testActor))
      processor1 ! Recover()
      processor1 ! "a"
      expectMsg("updated-a-1")
      processor1 ! "snap"
      expectMsg("snapped-a-1")
      2L to 7L foreach { i =>
        processor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorCNoRecover], "p11", testActor))
      processor2 ! Recover(toSequenceNr = 3L)
      expectMsg("offered-a-1")
      expectMsg("updated-a-2")
      expectMsg("updated-a-3")
      processor2 ! "d"
      expectMsg("updated-d-8")
    }
    "recover from a snapshot without follow-up messages inside a partition" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p12", testActor))
      processor1 ! "a"
      expectMsg("updated-a-1")
      processor1 ! "snap"
      expectMsg("snapped-a-1")

      val processor2 = system.actorOf(Props(classOf[ProcessorC], "p12", testActor))
      expectMsg("offered-a-1")
      processor2 ! "b"
      expectMsg("updated-b-2")
    }
    "recover from a snapshot without follow-up messages at a partition boundary (where next partition is invalid)" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p13", testActor))
      1L to 5L foreach { i =>
        processor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }
      processor1 ! "snap"
      expectMsg("snapped-a-5")

      val processor2 = system.actorOf(Props(classOf[ProcessorC], "p13", testActor))
      expectMsg("offered-a-5")
      processor2 ! "b"
      expectMsg("updated-b-6")
    }
    "recover from a snapshot without follow-up messages at a partition boundary (where next partition contains a message marked as deleted)" in {
      val deleteProbe = TestProbe()
      subscribeToBatchDeletion(deleteProbe)

      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p14", testActor))
      1L to 5L foreach { i =>
        processor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }
      processor1 ! "snap"
      expectMsg("snapped-a-5")

      processor1 ! "a"
      expectMsg("updated-a-6")

      processor1 ! Delete(6L, false)
      awaitBatchDeletion(deleteProbe)

      val processor2 = system.actorOf(Props(classOf[ProcessorC], "p14", testActor))
      expectMsg("offered-a-5")
      processor2 ! "b"
      expectMsg("updated-b-7")
    }
    "recover from a snapshot without follow-up messages at a partition boundary (where next partition contains a permanently deleted message)" in {
      val deleteProbe = TestProbe()
      subscribeToBatchDeletion(deleteProbe)

      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p15", testActor))
      1L to 5L foreach { i =>
        processor1 ! "a"
        expectMsg(s"updated-a-${i}")
      }
      processor1 ! "snap"
      expectMsg("snapped-a-5")

      processor1 ! "a"
      expectMsg("updated-a-6")

      processor1 ! Delete(6L, true)
      awaitBatchDeletion(deleteProbe)

      val processor2 = system.actorOf(Props(classOf[ProcessorC], "p15", testActor))
      expectMsg("offered-a-5")
      processor2 ! "b"
      expectMsg("updated-b-6") // sequence number of permanently deleted message can be re-used
    }
    "properly recover after all messages have been deleted" in {
      val deleteProbe = TestProbe()
      subscribeToBatchDeletion(deleteProbe)

      val p = system.actorOf(Props(classOf[ProcessorA], "p16"))

      p ! "a"
      expectMsgAllOf("a", 1L, false)

      p ! Delete(1L, true)
      awaitBatchDeletion(deleteProbe)

      val r = system.actorOf(Props(classOf[ProcessorA], "p16"))

      r ! "b"
      expectMsgAllOf("b", 1L, false)
    }
  }
}
