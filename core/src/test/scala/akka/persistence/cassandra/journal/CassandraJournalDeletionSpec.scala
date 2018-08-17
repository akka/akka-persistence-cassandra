/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.journal

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.PoisonPill
import akka.actor.Props
import akka.persistence.DeleteMessagesFailure
import akka.persistence.DeleteMessagesSuccess
import akka.persistence.PersistentActor
import akka.persistence.RecoveryCompleted
import akka.persistence.cassandra.CassandraLifecycle
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

object CassandraJournalDeletionSpec {
  case class PersistMe(msg: Long)
  case class DeleteTo(sequenceNr: Long)
  case object Ack
  case object GetRecoveredEvents

  case class RecoveredEvents(events: Seq[Any])

  case class Deleted(sequenceNr: Long)

  class PAThatDeletes(
    val persistenceId:            String,
    deleteSuccessProbe:           ActorRef,
    deleteFailProbe:              ActorRef,
    override val journalPluginId: String   = "cassandra-journal"
  )
    extends PersistentActor {

    var recoveredEvents: List[Any] = List.empty

    override def receiveRecover: Receive = {
      case event =>
        recoveredEvents = event :: recoveredEvents
    }

    var lastDeletedTo: Long = 0

    override def receiveCommand: Receive = {
      case p: PersistMe => persist(p) { _ =>
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

  val config = ConfigFactory.parseString(
    """
      akka.loglevel = INFO

      cassandra-journal {
        target-partition-size = 3
        keyspace = "DeletionSpecMany"
      }
    """
  ).withFallback(CassandraLifecycle.config)
}

class CassandraJournalDeletionSpec extends TestKit(ActorSystem(
  "CassandraJournalDeletionSpec",
  CassandraJournalDeletionSpec.config
))
  with ImplicitSender with WordSpecLike with Matchers with CassandraLifecycle {
  import CassandraJournalDeletionSpec._

  override def systemName: String = "CassandraJournalDeletionSpec"

  "Cassandra deletion" must {

    "handle deletes over many partitions" in {
      val deleteSuccess = TestProbe()
      val deleteFail = TestProbe()
      val props = Props(new PAThatDeletes("p3", deleteSuccess.ref, deleteFail.ref))
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
}
