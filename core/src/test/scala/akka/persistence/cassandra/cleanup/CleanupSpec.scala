/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.cleanup

import java.time.LocalDateTime
import java.time.ZoneOffset

import akka.actor.ActorRef
import akka.actor.Props
import akka.persistence.PersistentActor
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SnapshotOffer
import akka.persistence.cassandra.CassandraSpec
import akka.persistence.cassandra.query.firstBucketFormatter
import akka.persistence.journal.Tagged
import akka.persistence.query.NoOffset
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory

object CleanupSpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)
  val config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.persistence.cassandra.cleanup {
      log-progress-every = 2
    }
    akka.persistence.cassandra.events-by-tag {
      first-time-bucket = "${today.minusDays(5).format(firstBucketFormatter)}"
      eventual-consistency-delay = 1s
    }
  """)

  case object PersistEvent
  final case class PersistTaggedEvent(tag: String)
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
      case PersistTaggedEvent(tag) =>
        val seqNr = lastSequenceNr + 1
        persist(Tagged(s"evt-$seqNr", Set(tag))) { _ =>
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

      // also delete from all_persistence_ids
      queries.currentPersistenceIds().runWith(Sink.seq).futureValue should not contain (pid)

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

    "delete tagged events for one persistenceId" in {
      val pid = nextPid
      val p = system.actorOf(TestActor.props(pid))
      (1 to 10).foreach { _ =>
        p ! PersistTaggedEvent("tag-a")
        expectMsgType[Ack]
      }

      system.stop(p)

      queries.currentEventsByTag(tag = "tag-a", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(10)

      val cleanup = new Cleanup(system)
      cleanup.deleteAllTaggedEvents(pid).futureValue

      queries.currentEventsByTag(tag = "tag-a", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(0)
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
        p ! PersistTaggedEvent("tag-a")
        expectMsgType[Ack]
      }
      p ! Snap
      expectMsgType[Ack]
      (1 to 3).foreach { _ =>
        p ! PersistTaggedEvent("tag-b")
        expectMsgType[Ack]
      }

      queries.currentEventsByTag(tag = "tag-a", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(2)
      queries.currentEventsByTag(tag = "tag-b", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(3)

      system.stop(p)

      val cleanup = new Cleanup(system)
      cleanup.deleteAll(pid, neverUsePersistenceIdAgain = true).futureValue

      // also delete from all_persistence_ids
      queries.currentPersistenceIds().runWith(Sink.seq).futureValue should not contain (pid)

      val p2 = system.actorOf(TestActor.props(pid))
      p2 ! GetRecoveredState
      expectMsg(RecoveredState("", Nil, 0L))

      queries.currentEventsByTag(tag = "tag-a", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(0)
      queries.currentEventsByTag(tag = "tag-b", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(0)
    }

    "delete all for several persistenceId" in {
      val pidA = nextPid
      val pidB = nextPid
      val pidC = nextPid
      val pA = system.actorOf(TestActor.props(pidA))
      val pB = system.actorOf(TestActor.props(pidB))
      val pC = system.actorOf(TestActor.props(pidC))
      (1 to 3).foreach { i =>
        pA ! PersistTaggedEvent("tag-a")
        expectMsgType[Ack]
        pB ! PersistEvent
        expectMsgType[Ack]
        pC ! PersistTaggedEvent("tag-c")
        expectMsgType[Ack]
      }
      pA ! Snap
      expectMsgType[Ack]
      pB ! Snap
      expectMsgType[Ack]
      (1 to 2).foreach { _ =>
        pA ! PersistTaggedEvent("tag-a")
        expectMsgType[Ack]
        pB ! PersistEvent
        expectMsgType[Ack]
      }
      pA ! Snap
      expectMsgType[Ack]
      (1 to 3).foreach { _ =>
        pA ! PersistTaggedEvent("tag-a")
        expectMsgType[Ack]
        pC ! PersistTaggedEvent("tag-c")
        expectMsgType[Ack]
      }

      queries.currentEventsByTag(tag = "tag-a", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(8)
      queries.currentEventsByTag(tag = "tag-c", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(6)

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

      queries.currentEventsByTag(tag = "tag-a", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(0)
      queries.currentEventsByTag(tag = "tag-c", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(0)

      // also delete from all_persistence_ids
      val foundPids = queries.currentPersistenceIds().runWith(Sink.seq).futureValue.toSet
      Set(pidA, pidB, pidC).intersect(foundPids) should ===(Set.empty)
    }

    "delete all for many persistenceId, many events, many snapshots" in {
      val nrPids = 100
      val nrEvents = 50
      val snapEvery = 10

      val pids = Vector.fill(nrPids)(nextPid)
      val actors = pids.map { pid =>
        val p = system.actorOf(TestActor.props(pid))
        (1 to nrEvents).foreach { n =>
          p ! PersistEvent
          expectMsgType[Ack]
          if (n % snapEvery == 0) {
            p ! Snap
            expectMsgType[Ack]
          }
        }
        p
      }

      actors.foreach(system.stop)

      val conf = ConfigFactory.parseString("""
        log-progress-every = 10
      """).withFallback(system.settings.config.getConfig("akka.persistence.cassandra.cleanup"))
      val cleanup = new Cleanup(system, new CleanupSettings(conf))
      cleanup.deleteAll(pids, neverUsePersistenceIdAgain = true).futureValue

      // also delete from all_persistence_ids
      val foundPids = queries.currentPersistenceIds().runWith(Sink.seq).futureValue.toSet
      pids.toSet.intersect(foundPids) should ===(Set.empty)

      val p2 = system.actorOf(TestActor.props(pids.last))
      p2 ! GetRecoveredState
      expectMsg(RecoveredState("", Nil, 0L))
    }

  }

}
