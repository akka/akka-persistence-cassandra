package akka.persistence.journal.cassandra

import akka.actor._
import akka.persistence._
import akka.testkit._

import com.typesafe.config.ConfigFactory

import org.scalatest._

object CassandraJournalSpec {
  val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "cassandra-journal"
      |akka.persistence.snapshot-store.local.dir = "target/snapshots"
      |akka.persistence.publish-plugin-commands = on
      |cassandra-journal.max-partition-size = 5
      |cassandra-journal.max-result-size = 3
    """.stripMargin)

  case class Delete(snr: Long, permanent: Boolean)

  class ProcessorA(override val processorId: String) extends Processor {
    def receive = {
      case Persistent(payload, sequenceNr) =>
        sender ! payload
        sender ! sequenceNr
        sender ! recoveryRunning
      case Delete(sequenceNr, permanent) =>
        deleteMessage(sequenceNr, permanent)
    }
  }

  class ProcessorB(override val processorId: String) extends Processor {
    val destination = context.actorOf(Props[Destination])
    val channel = context.actorOf(Channel.props("channel"))

    def receive = {
      case p: Persistent => channel forward Deliver(p, destination)
    }
  }

  class ProcessorC(override val processorId: String, probe: ActorRef) extends Processor {
    var last: String = _

    def receive = {
      case Persistent(payload: String, sequenceNr) =>
        last = s"${payload}-${sequenceNr}"
        probe ! s"updated-${last}"
      case "snap" =>
        saveSnapshot(last)
      case SaveSnapshotSuccess(_) =>
        probe ! s"snapped-${last}"
      case SnapshotOffer(_, snapshot: String) =>
        last = snapshot
        probe ! s"offered-${last}"
      case Delete(sequenceNr, permanent) =>
        deleteMessage(sequenceNr, permanent)
    }
  }

  class ProcessorCNoRecover(override val processorId: String, probe: ActorRef) extends ProcessorC(processorId, probe) {
    override def preStart() = ()
  }

  class Destination extends Actor {
    def receive = {
      case cp @ ConfirmablePersistent(payload, sequenceNr, _) =>
        sender ! s"${payload}-${sequenceNr}"
        cp.confirm()
    }
  }
}

import CassandraJournalSpec._

class CassandraJournalSpec extends TestKit(ActorSystem("test", config)) with ImplicitSender with WordSpecLike with Matchers with CassandraCleanup {
  "A Cassandra journal" should {
    "write and replay messages" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorA], "p1"))
      1L to 16L foreach { i =>
        processor1 ! Persistent(s"a-${i}")
        expectMsgAllOf(s"a-${i}", i, false)
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorA], "p1"))
      1L to 16L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, true)
      }

      processor2 ! Persistent("b")
      expectMsgAllOf("b", 17L, false)
    }
    "write delivery confirmations" in {
      val confirmProbe = TestProbe()
      subscribeToConfirmation(confirmProbe)

      val processor1 = system.actorOf(Props(classOf[ProcessorB], "p4"))
      1L to 16L foreach { i =>
        processor1 ! Persistent("a")
        awaitConfirmation(confirmProbe)
        expectMsg(s"a-${i}")
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorB], "p4"))
      processor2 ! Persistent("b")
      awaitConfirmation(confirmProbe)
      expectMsg("b-17")
    }
    "recover from a snapshot with follow-up messages" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p5", testActor))
      processor1 ! Persistent("a")
      expectMsg("updated-a-1")
      processor1 ! "snap"
      expectMsg("snapped-a-1")
      processor1 ! Persistent("b")
      expectMsg("updated-b-2")

      system.actorOf(Props(classOf[ProcessorC], "p5", testActor))
      expectMsg("offered-a-1")
      expectMsg("updated-b-2")
    }
    "recover from a snapshot with follow-up messages and an upper bound" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorCNoRecover], "p5a", testActor))
      processor1 ! Recover()
      processor1 ! Persistent("a")
      expectMsg("updated-a-1")
      processor1 ! "snap"
      expectMsg("snapped-a-1")
      2L to 7L foreach { i =>
        processor1 ! Persistent("a")
        expectMsg(s"updated-a-${i}")
      }

      val processor2 = system.actorOf(Props(classOf[ProcessorCNoRecover], "p5a", testActor))
      processor2 ! Recover(toSequenceNr = 3L)
      expectMsg("offered-a-1")
      expectMsg("updated-a-2")
      expectMsg("updated-a-3")
      processor2 ! Persistent("d")
      expectMsg("updated-d-8")
    }
    "recover from a snapshot without follow-up messages inside a partition" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p6", testActor))
      processor1 ! Persistent("a")
      expectMsg("updated-a-1")
      processor1 ! "snap"
      expectMsg("snapped-a-1")

      val processor2 = system.actorOf(Props(classOf[ProcessorC], "p6", testActor))
      expectMsg("offered-a-1")
      processor2 ! Persistent("b")
      expectMsg("updated-b-2")
    }
    "recover from a snapshot without follow-up messages at a partition boundary (where next partition is invalid)" in {
      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p7", testActor))
      1L to 5L foreach { i =>
        processor1 ! Persistent("a")
        expectMsg(s"updated-a-${i}")
      }
      processor1 ! "snap"
      expectMsg("snapped-a-5")

      val processor2 = system.actorOf(Props(classOf[ProcessorC], "p7", testActor))
      expectMsg("offered-a-5")
      processor2 ! Persistent("b")
      expectMsg("updated-b-6")
    }
    "recover from a snapshot without follow-up messages at a partition boundary (where next partition contains a message marked as deleted)" in {
      val deleteProbe = TestProbe()
      subscribeToDeletion(deleteProbe)

      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p8", testActor))
      1L to 5L foreach { i =>
        processor1 ! Persistent("a")
        expectMsg(s"updated-a-${i}")
      }
      processor1 ! "snap"
      expectMsg("snapped-a-5")

      processor1 ! Persistent("a")
      expectMsg("updated-a-6")

      processor1 ! Delete(6L, false)
      awaitDeletion(deleteProbe)

      val processor2 = system.actorOf(Props(classOf[ProcessorC], "p8", testActor))
      expectMsg("offered-a-5")
      processor2 ! Persistent("b")
      expectMsg("updated-b-7")
    }
    "recover from a snapshot without follow-up messages at a partition boundary (where next partition contained a permanently deleted message)" in {
      val deleteProbe = TestProbe()
      subscribeToDeletion(deleteProbe)

      val processor1 = system.actorOf(Props(classOf[ProcessorC], "p9", testActor))
      1L to 5L foreach { i =>
        processor1 ! Persistent("a")
        expectMsg(s"updated-a-${i}")
      }
      processor1 ! "snap"
      expectMsg("snapped-a-5")

      processor1 ! Persistent("a")
      expectMsg("updated-a-6")

      processor1 ! Delete(6L, true)
      awaitDeletion(deleteProbe)

      val processor2 = system.actorOf(Props(classOf[ProcessorC], "p9", testActor))
      expectMsg("offered-a-5")
      processor2 ! Persistent("b")
      expectMsg("updated-b-6") // sequence number of permanently deleted message can be re-used
    }

    "not replay messages marked as deleted" in {
      testDelete("p2", false)
    }
    "not replay permanently deleted messages" in {
      testDelete("p3", true)
    }

    def testDelete(processorId: String, permanent: Boolean) {
      val deleteProbe = TestProbe()
      subscribeToDeletion(deleteProbe)

      val processor1 = system.actorOf(Props(classOf[ProcessorA], processorId))
      1L to 16L foreach { i =>
        processor1 ! Persistent(s"a-${i}")
        expectMsgAllOf(s"a-${i}", i, false)
      }

      // delete single message in partition
      processor1 ! Delete(12L, permanent)
      awaitDeletion(deleteProbe)

      system.actorOf(Props(classOf[ProcessorA], processorId))
      1L to 16L foreach { i =>
        if (i != 12L) expectMsgAllOf(s"a-${i}", i, true)
      }

      // delete whole partition
      6L to 10L foreach { i =>
        processor1 ! Delete(i, permanent)
        awaitDeletion(deleteProbe)
      }

      system.actorOf(Props(classOf[ProcessorA], processorId))
      1L to 5L foreach { i =>
        expectMsgAllOf(s"a-${i}", i, true)
      }
      11L to 16L foreach { i =>
        if (i != 12L) expectMsgAllOf(s"a-${i}", i, true)
      }
    }
  }

  def subscribeToConfirmation(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.Confirm])

  def subscribeToDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.Delete])

  def awaitConfirmation(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.Confirm]

  def awaitDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.Delete]
}
