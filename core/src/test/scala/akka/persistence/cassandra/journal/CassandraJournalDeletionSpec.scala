/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.actor.{ ActorRef, PoisonPill, Props }
import akka.persistence.{ DeleteMessagesFailure, DeleteMessagesSuccess, PersistentActor }
import akka.persistence.cassandra.CassandraSpec
import akka.testkit.TestProbe

object CassandraJournalDeletionSpec {
  case class PersistMe(msg: Long)
  case class DeleteTo(sequenceNr: Long)
  case object Ack

  class PAThatDeletes(val persistenceId: String, deleteSuccessProbe: ActorRef) extends PersistentActor {
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
        deleteSuccessProbe ! to
      case DeleteMessagesFailure(t, to) =>
        context.system.log.error(t, "Failed to delete to {}", to)
        context.stop(self)
    }
  }
}

class CassandraJournalDeletionSpec extends CassandraSpec(
  """
    akka.loglevel = DEBUG
    cassandra-journal {
      # increase this to allow more concurrent deletes to make it more likely to find issues
      max-concurrent-deletes = 100
    }
  """
) {

  import CassandraJournalDeletionSpec._

  "Cassandra deletion" must {
    "allow concurrent deletes" in {
      val probe = TestProbe()
      val p1 = system.actorOf(Props(new PAThatDeletes("p1", probe.ref)))
      (1 to 100).foreach { i =>
        p1 ! PersistMe(i)
        expectMsg(Ack)
      }

      (1 to 99).foreach { i =>
        p1 ! DeleteTo(i)
      }

      // Should be in order, previously they could over take each other.
      (1L to 99L).foreach { i =>
        probe.expectMsg(i)
      }
      probe.expectNoMessage()

      p1 ! PoisonPill

      // Recovery should not find a missing sequence nr
      val p1TakeTwo = system.actorOf(Props(new PAThatDeletes("p1", probe.ref)))
      p1TakeTwo ! PersistMe(101)
      expectMsg(Ack)
    }
  }
}
