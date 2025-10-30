/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.cassandra.journal

import akka.actor.{ ActorRef, PoisonPill, Props }
import akka.persistence.{ DeleteMessagesFailure, DeleteMessagesSuccess, PersistentActor, RecoveryCompleted }
import akka.persistence.cassandra.CassandraSpec
import akka.testkit.TestProbe
import scala.collection.immutable
import scala.concurrent.duration._

import akka.testkit.EventFilter

object CassandraJournalDeletionSpec {
  case class PersistMe(msg: Long)
  case class DeleteTo(sequenceNr: Long)
  case class Ack(sequenceNr: Long)
  case object GetRecoveredEvents

  case class RecoveredEvents(events: Seq[Any])

  case class Deleted(sequenceNr: Long)

  class PAThatDeletes(
      val persistenceId: String,
      deleteSuccessProbe: ActorRef,
      deleteFailProbe: ActorRef,
      override val journalPluginId: String = "akka.persistence.cassandra.journal")
      extends PersistentActor {

    var recoveredEvents: List[Any] = List.empty

    override def receiveRecover: Receive = {
      case event =>
        recoveredEvents = event :: recoveredEvents
    }

    var lastDeletedTo: Long = 0

    override def receiveCommand: Receive = {
      case p: PersistMe =>
        persist(p) { _ =>
          sender() ! Ack(lastSequenceNr)
        }
      case GetRecoveredEvents =>
        sender() ! RecoveredEvents(recoveredEvents.reverse)
      case DeleteTo(to) =>
        deleteMessages(to)
      case DeleteMessagesSuccess(to) =>
        context.system.log.debug("Deleted to: {}", to)
        lastDeletedTo = to
        deleteSuccessProbe ! Deleted(to)
      case DeleteMessagesFailure(t, to) =>
        deleteFailProbe ! t
        context.system.log.error(t, "Failed to delete to {}", to)
        context.stop(self)
    }
  }
}

class CassandraJournalDeletionSpec extends CassandraSpec(s"""
    akka.loggers = ["akka.testkit.TestEventListener"]
    akka.log-dead-letters = off
    akka.persistence.cassandra.journal.max-concurrent-deletes = 100

    cassandra-plugin-low-concurrent-deletes = $${akka.persistence.cassandra}
    cassandra-plugin-low-concurrent-deletes {
      journal.max-concurrent-deletes = 5
    }

    cassandra-plugin-small-partition-size = $${akka.persistence.cassandra}
    cassandra-plugin-small-partition-size {
      journal.target-partition-size = 3
      journal.keyspace = "DeletionSpecMany"
    }
    
    cassandra-plugin-no-delete = $${akka.persistence.cassandra}
    cassandra-plugin-no-delete {
      journal.support-deletes = off
    }
  """) {

  import CassandraJournalDeletionSpec._

  override def keyspaces(): Set[String] = super.keyspaces().union(Set("DeletionSpecMany"))

  "Cassandra deletion" must {
    "allow recovery after delete without snapshot" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val pid = nextId()
      val p1 = system.actorOf(Props(new PAThatDeletes(pid, deleteSuccess.ref, deleteFail.ref)))
      watch(p1)
      (1 to 3).foreach { i =>
        p1 ! PersistMe(i)
        expectMsgType[Ack]
      }
      p1 ! DeleteTo(2)
      deleteSuccess.expectMsg(Deleted(2))

      p1 ! PoisonPill
      expectTerminated(p1)

      val p1Strike2 = system.actorOf(Props(new PAThatDeletes(pid, deleteSuccess.ref, deleteFail.ref)))
      p1Strike2 ! GetRecoveredEvents
      expectMsg(RecoveredEvents(List(PersistMe(3), RecoveryCompleted)))
    }

    "allow concurrent deletes" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val p1 = system.actorOf(Props(new PAThatDeletes("p1", deleteSuccess.ref, deleteFail.ref)))
      (1 to 100).foreach { i =>
        p1 ! PersistMe(i)
        expectMsgType[Ack]
      }

      (1 to 99).foreach { i =>
        p1 ! DeleteTo(i)
      }

      // The AsyncWriteJournal does not guarantee that DeleteSuccess are delivered in the order
      // that they are completed by the journal implementation so can't assert this reliably
      (1L to 99L).map { _ =>
        deleteSuccess.expectMsgType[Deleted].sequenceNr
      }.toSet shouldEqual (1L to 99L).toSet
      deleteSuccess.expectNoMessage(100.millis)

      p1 ! PoisonPill

      // Recovery should not find a missing sequence nr
      val p1TakeTwo = system.actorOf(Props(new PAThatDeletes("p1", deleteSuccess.ref, deleteFail.ref)))
      p1TakeTwo ! GetRecoveredEvents
      expectMsg(RecoveredEvents(List(PersistMe(100), RecoveryCompleted)))
    }

    "fail fast if too many concurrent deletes" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val p1 = system.actorOf(Props(
        new PAThatDeletes("p2", deleteSuccess.ref, deleteFail.ref, "cassandra-plugin-low-concurrent-deletes.journal")))

      (1 to 100).foreach { i =>
        p1 ! PersistMe(i)
        expectMsgType[Ack]
      }

      (1 to 99).foreach { i =>
        p1 ! DeleteTo(i)
      }

      val msg = deleteFail.expectMsgType[RuntimeException]
      msg.getMessage shouldEqual "Over 5 outstanding deletes for persistenceId p2"

      // Does't matter how many as long as they are all in order
      val successes: immutable.Seq[Long] = deleteSuccess.receiveWhile(max = 100.millis) {
        case Deleted(i) => i
      }
      successes shouldEqual successes.sorted
    }

    "handle deletes of all events" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val props =
        Props(new PAThatDeletes("p3", deleteSuccess.ref, deleteFail.ref))
      val p1 = system.actorOf(props)
      (1 to 17).foreach { i =>
        p1 ! PersistMe(i)
        expectMsg(Ack(i))
      }

      p1 ! DeleteTo(17)
      deleteSuccess.expectMsg(Deleted(17))

      p1 ! PoisonPill

      // Recovery should not find a deleted sequence nr, and use next sequence nr for persist
      val p1TakeTwo = system.actorOf(props)
      p1TakeTwo ! GetRecoveredEvents
      expectMsg(RecoveredEvents(List(RecoveryCompleted)))
      p1TakeTwo ! PersistMe(18)
      expectMsg(Ack(18))
    }

    "handle deletes over many partitions" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val props =
        Props(
          new PAThatDeletes("p4", deleteSuccess.ref, deleteFail.ref, "cassandra-plugin-small-partition-size.journal"))
      val p1 = system.actorOf(props)
      (1 to 100).foreach { i =>
        p1 ! PersistMe(i)
        expectMsgType[Ack]
      }

      p1 ! DeleteTo(10)
      deleteSuccess.expectMsg(Deleted(10))

      p1 ! DeleteTo(20)
      deleteSuccess.expectMsg(Deleted(20))

      p1 ! DeleteTo(98)
      deleteSuccess.expectMsg(Deleted(98))

      p1 ! PoisonPill

      // Recovery should not find a deleted sequence nr
      val p1TakeTwo = system.actorOf(props)
      p1TakeTwo ! GetRecoveredEvents
      expectMsg(RecoveredEvents(List(PersistMe(99), PersistMe(100), RecoveryCompleted)))
      p1TakeTwo ! PersistMe(101)
      expectMsg(Ack(101))

      // Delete all with Long.MaxValue
      p1TakeTwo ! DeleteTo(Long.MaxValue)
      deleteSuccess.expectMsg(Deleted(Long.MaxValue))

      p1TakeTwo ! PoisonPill

      val p1TakeThree = system.actorOf(props)
      p1TakeThree ! GetRecoveredEvents
      expectMsg(RecoveredEvents(List(RecoveryCompleted)))
      p1TakeThree ! PersistMe(102)
      expectMsg(Ack(102))
    }

    "handle deletes of all events over many partitions" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val props =
        Props(
          new PAThatDeletes("p5", deleteSuccess.ref, deleteFail.ref, "cassandra-plugin-small-partition-size.journal"))
      val p1 = system.actorOf(props)
      (1 to 100).foreach { i =>
        p1 ! PersistMe(i)
        expectMsgType[Ack]
      }

      p1 ! DeleteTo(100)
      deleteSuccess.expectMsg(Deleted(100))

      p1 ! PoisonPill

      // Recovery should not find a deleted sequence nr, and use next sequence nr for persist
      val p1TakeTwo = system.actorOf(props)
      p1TakeTwo ! GetRecoveredEvents
      expectMsg(RecoveredEvents(List(RecoveryCompleted)))
      p1TakeTwo ! PersistMe(101)
      expectMsg(Ack(101))
    }

    "recover with support-deletes=off" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val p1 =
        system.actorOf(
          Props(new PAThatDeletes("p6", deleteSuccess.ref, deleteFail.ref, "cassandra-plugin-no-delete.journal")))

      (1 to 3).foreach { i =>
        p1 ! PersistMe(i)
        expectMsgType[Ack]
      }

      p1 ! PoisonPill

      val p1TakeTwo = system.actorOf(Props(new PAThatDeletes("p6", deleteSuccess.ref, deleteFail.ref)))
      p1TakeTwo ! GetRecoveredEvents
      expectMsg(RecoveredEvents(List(PersistMe(1), PersistMe(2), PersistMe(3), RecoveryCompleted)))
    }

    "fail if attempt to delete with support-deletes=off" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val p1 =
        system.actorOf(
          Props(new PAThatDeletes("p7", deleteSuccess.ref, deleteFail.ref, "cassandra-plugin-no-delete.journal")))

      (1 to 3).foreach { i =>
        p1 ! PersistMe(i)
        expectMsgType[Ack]
      }

      EventFilter.error(start = "Failed to delete to 2", occurrences = 1).intercept {
        p1 ! DeleteTo(2)
      }

      deleteFail.expectMsgType[IllegalArgumentException]
    }

  }

}
