/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.cleanup

import akka.actor.ActorRef
import akka.actor.Props
import akka.persistence.PersistentActor
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SnapshotOffer
import akka.persistence.cassandra.CassandraSpec
import com.typesafe.config.ConfigFactory

object CleanupSpec {
  val config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
  """)

  case object PersistEvent
  final case class Ack(sequenceNr: Long)
  case object GetRecoveredState
  final case class RecoveredState(snap: String, events: Seq[String], sequenceNr: Long)
  case object Snap

  object TestActor {
    def props(persistenceId: String): Props =
      Props(new TestActor(persistenceId))
  }

  class TestActor(override val persistenceId: String) extends PersistentActor {

    var recoveredSnap: String = ""
    var replayedEvents: List[String] = List.empty
    var lastSnapSender: ActorRef = context.system.deadLetters

    override def receiveRecover: Receive = {
      case SnapshotOffer(_, snap: String) =>
        recoveredSnap = snap
      case event: String =>
        replayedEvents = event :: replayedEvents
    }

    override def receiveCommand: Receive = {
      case PersistEvent =>
        val seqNr = lastSequenceNr + 1
        persist(s"evt-$seqNr") { _ =>
          sender() ! Ack(seqNr)
        }
      case GetRecoveredState =>
        sender() ! RecoveredState(recoveredSnap, replayedEvents.reverse, lastSequenceNr)
      case Snap =>
        lastSnapSender = sender()
        saveSnapshot(s"snap-$lastSequenceNr")
      case SaveSnapshotSuccess(meta) =>
        lastSnapSender ! Ack(meta.sequenceNr)
    }
  }
}

class CleanupSpec extends CassandraSpec(CleanupSpec.config) {

  import CleanupSpec._

  "Cassandra cleanup" must {
    "delete events for one persistenceId" in {
      val pid = nextPid
      val p = system.actorOf(TestActor.props(pid))
      (1 to 10).foreach { _ =>
        p ! PersistEvent
        expectMsgType[Ack]
      }

      system.stop(p)

      val cleanup = new Cleanup(system)
      cleanup.deleteAllEvents(pid, neverUsePersistenceIdAgain = true).futureValue

      val p2 = system.actorOf(TestActor.props(pid))
      p2 ! GetRecoveredState
      expectMsg(RecoveredState("", Nil, 0L))
    }

    "delete events for one persistenceId, but keep seqNr" in {
      val pid = nextPid
      val p = system.actorOf(TestActor.props(pid))
      (1 to 10).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }

      system.stop(p)

      val cleanup = new Cleanup(system)
      cleanup.deleteAllEvents(pid, neverUsePersistenceIdAgain = false).futureValue

      val p2 = system.actorOf(TestActor.props(pid))
      p2 ! GetRecoveredState
      expectMsg(RecoveredState("", Nil, 10L))
    }

    "delete snapshots for one persistenceId" in {
      val pid = nextPid
      val p = system.actorOf(TestActor.props(pid))
      (1 to 3).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      p ! Snap
      expectMsgType[Ack]
      (1 to 5).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }

      system.stop(p)

      val cleanup = new Cleanup(system)
      cleanup.deleteAllSnapshots(pid).futureValue

      val p2 = system.actorOf(TestActor.props(pid))
      p2 ! GetRecoveredState
      expectMsg(RecoveredState("", (1 to 8).map(n => s"evt-$n"), 8L))
    }

    "delete all for one persistenceId" in {
      val pid = nextPid
      val p = system.actorOf(TestActor.props(pid))
      (1 to 3).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      p ! Snap
      expectMsgType[Ack]
      (1 to 2).foreach { _ =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      p ! Snap
      expectMsgType[Ack]
      (1 to 3).foreach { _ =>
        p ! PersistEvent
        expectMsgType[Ack]
      }

      system.stop(p)

      val cleanup = new Cleanup(system)
      cleanup.deleteAll(pid, neverUsePersistenceIdAgain = true).futureValue

      val p2 = system.actorOf(TestActor.props(pid))
      p2 ! GetRecoveredState
      expectMsg(RecoveredState("", Nil, 0L))
    }

    "delete all for several persistenceId" in {
      val pidA = nextPid
      val pidB = nextPid
      val pidC = nextPid
      val pA = system.actorOf(TestActor.props(pidA))
      val pB = system.actorOf(TestActor.props(pidB))
      val pC = system.actorOf(TestActor.props(pidC))
      (1 to 3).foreach { i =>
        pA ! PersistEvent
        expectMsgType[Ack]
        pB ! PersistEvent
        expectMsgType[Ack]
        pC ! PersistEvent
        expectMsgType[Ack]
      }
      pA ! Snap
      expectMsgType[Ack]
      pB ! Snap
      expectMsgType[Ack]
      (1 to 2).foreach { _ =>
        pA ! PersistEvent
        expectMsgType[Ack]
        pB ! PersistEvent
        expectMsgType[Ack]
      }
      pA ! Snap
      expectMsgType[Ack]
      (1 to 3).foreach { _ =>
        pA ! PersistEvent
        expectMsgType[Ack]
        pC ! PersistEvent
        expectMsgType[Ack]
      }

      system.stop(pA)

      val cleanup = new Cleanup(system)
      cleanup.deleteAll(List(pidA, pidB, pidC), neverUsePersistenceIdAgain = true).futureValue

      val pA2 = system.actorOf(TestActor.props(pidA))
      pA2 ! GetRecoveredState
      expectMsg(RecoveredState("", Nil, 0L))

      val pB2 = system.actorOf(TestActor.props(pidB))
      pB2 ! GetRecoveredState
      expectMsg(RecoveredState("", Nil, 0L))

      val pC2 = system.actorOf(TestActor.props(pidC))
      pC2 ! GetRecoveredState
      expectMsg(RecoveredState("", Nil, 0L))
    }

  }

}
