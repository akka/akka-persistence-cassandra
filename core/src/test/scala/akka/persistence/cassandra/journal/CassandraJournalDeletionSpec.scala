/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.{ ActorRef, PoisonPill, Props }
import akka.persistence.{ DeleteMessagesFailure, DeleteMessagesSuccess, PersistentActor, RecoveryCompleted }
import akka.persistence.cassandra.CassandraSpec
import akka.testkit.TestProbe

import scala.collection.immutable
import scala.concurrent.duration._

object CassandraJournalDeletionSpec {
  case class PersistMe(msg: Long)
  case class DeleteTo(sequenceNr: Long)
  case object Ack
  case object GetRecoveredEvents

  case class RecoveredEvents(events: Seq[Any])

  case class Deleted(sequenceNr: Long)

  class PAThatDeletes(
      val persistenceId: String,
      deleteSuccessProbe: ActorRef,
      deleteFailProbe: ActorRef,
      override val journalPluginId: String = "cassandra-journal")
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
          sender() ! Ack
        }
      case GetRecoveredEvents =>
        sender() ! RecoveredEvents(recoveredEvents.reverse)
      case DeleteTo(to) => deleteMessages(to)
      case DeleteMessagesSuccess(to) =>
        context.system.log.debug("Deleted to: {}", to)
        require(to > lastDeletedTo, s"Received deletes in wrong order. Last ${lastDeletedTo}. Current: ${to}")
        lastDeletedTo = to
        deleteSuccessProbe ! Deleted(to)
      case DeleteMessagesFailure(t, to) =>
        deleteFailProbe ! t
        context.system.log.error(t, "Failed to delete to {}", to)
        context.stop(self)
    }
  }
}

class CassandraJournalDeletionSpec
    extends CassandraSpec("""
    akka.loglevel = DEBUG
    cassandra-journal.max-concurrent-deletes = 100

    cassandra-journal-low-concurrent-deletes = ${cassandra-journal}
    cassandra-journal-low-concurrent-deletes {
      max-concurrent-deletes = 5
      query-plugin = cassandra-query-journal-low-concurrent-deletes
    }
    cassandra-query-journal-low-concurrent-deletes = ${cassandra-query-journal}
    cassandra-query-journal-low-concurrent-deletes {
      write-plugin = cassandra-journal-low-concurrent-deletes
    }

    cassandra-journal-small-partition-size = ${cassandra-journal}
    cassandra-journal-small-partition-size {
      target-partition-size = 3
      keyspace = "DeletionSpecMany"
      query-plugin = cassandra-query-journal-small-partition-size
    }
    cassandra-query-journal-small-partition-size = ${cassandra-query-journal}
    cassandra-query-journal-small-partition-size {
      write-plugin = cassandra-journal-small-partition-size
    }
  """) {

  import CassandraJournalDeletionSpec._

  "Cassandra deletion" must {
    "allow concurrent deletes" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val p1 = system.actorOf(Props(new PAThatDeletes("p1", deleteSuccess.ref, deleteFail.ref)))
      (1 to 100).foreach { i =>
        p1 ! PersistMe(i)
        expectMsg(Ack)
      }

      (1 to 99).foreach { i =>
        p1 ! DeleteTo(i)
      }

      // Should be in order, previously they could over take each other.
      (1L to 99L).foreach { i =>
        deleteSuccess.expectMsg(Deleted(i))
      }
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
      val p1 = system.actorOf(
        Props(new PAThatDeletes("p2", deleteSuccess.ref, deleteFail.ref, "cassandra-journal-low-concurrent-deletes")))

      (1 to 100).foreach { i =>
        p1 ! PersistMe(i)
        expectMsg(Ack)
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

    "handle deletes over many partitions" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val props =
        Props(new PAThatDeletes("p3", deleteSuccess.ref, deleteFail.ref, "cassandra-journal-small-partition-size"))
      val p1 = system.actorOf(props)
      (1 to 100).foreach { i =>
        p1 ! PersistMe(i)
        expectMsg(Ack)
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

      // Delete all with Long.MaxValue
      p1TakeTwo ! DeleteTo(Long.MaxValue)
      deleteSuccess.expectMsg(Deleted(Long.MaxValue))

      p1TakeTwo ! PoisonPill

      val p1TakeThree = system.actorOf(props)
      p1TakeThree ! GetRecoveredEvents
      expectMsg(RecoveredEvents(List(RecoveryCompleted)))
    }

  }

  override protected def externalCassandraCleanup(): Unit = {
    super.externalCassandraCleanup()
    cluster.connect().execute(s"drop keyspace if exists DeletionSpecMany")
  }
}
