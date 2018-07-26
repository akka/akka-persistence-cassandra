/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.{ ActorRef, PoisonPill, Props }
import akka.persistence.{ DeleteMessagesFailure, DeleteMessagesSuccess, PersistentActor }
import akka.persistence.cassandra.CassandraSpec
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import scala.collection.immutable
import scala.concurrent.duration._

object CassandraJournalDeletionSpec {
  case class PersistMe(msg: Long)
  case class DeleteTo(sequenceNr: Long)
  case object Ack

  case class Deleted(sequenceNr: Long)

  class PAThatDeletes(
    val persistenceId:            String,
    deleteSuccessProbe:           ActorRef,
    deleteFailProbe:              ActorRef,
    override val journalPluginId: String   = "cassandra-journal"
  )
    extends PersistentActor {

    override def receiveRecover: Receive = {
      case _ =>
    }

    var lastDeletedTo: Long = 0

    override def receiveCommand: Receive = {
      case p: PersistMe => persist(p) { _ =>
        sender() ! Ack
      }
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

class CassandraJournalDeletionSpec extends CassandraSpec(
  """
    akka.loglevel = DEBUG
    cassandra-journal.max-concurrent-deletes = 100

    cassandra-journal-low-concurrent-deletes = ${cassandra-journal}
    cassandra-journal-low-concurrent-deletes {
      max-concurrent-deletes = 5
    }
  """
) {

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
      p1TakeTwo ! PersistMe(101)
      expectMsg(Ack)
    }

    "fail fast if too many concurrent deletes" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val p1 = system.actorOf(Props(
        new PAThatDeletes("p2", deleteSuccess.ref, deleteFail.ref, "cassandra-journal-low-concurrent-deletes")
      ))

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
  }
}
