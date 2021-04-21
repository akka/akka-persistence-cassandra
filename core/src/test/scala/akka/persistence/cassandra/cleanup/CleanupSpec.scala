/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.cleanup

import java.time.LocalDateTime
import java.time.ZoneOffset
import akka.actor.ActorRef
import akka.actor.Props
import akka.persistence.{ PersistentActor, SaveSnapshotSuccess, SnapshotMetadata, SnapshotOffer }
import akka.persistence.cassandra.{ CassandraSpec, RequiresCassandraThree }
import akka.persistence.cassandra.query.{ firstBucketFormatter, DirectWriting }
import akka.persistence.journal.Tagged
import akka.persistence.query.NoOffset
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import org.scalatest.time.Milliseconds
import org.scalatest.time.Seconds
import org.scalatest.time.Span

object CleanupSpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)
  val config = ConfigFactory.parseString(s"""
    akka.loglevel = INFO
    akka.persistence.cassandra.cleanup {
      log-progress-every = 2
      dry-run = false
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

class CleanupSpec extends CassandraSpec(CleanupSpec.config) with DirectWriting {
  import CleanupSpec._

  override implicit val patience = PatienceConfig(timeout = Span(30, Seconds), interval = Span(100, Milliseconds))

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
      cleanup.deleteAllTaggedEvents(pid).futureValue // FIXME

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
      cleanup.deleteAll(pid, neverUsePersistenceIdAgain = true).futureValue // FIXME

      // also delete from all_persistence_ids
      queries.currentPersistenceIds().runWith(Sink.seq).futureValue should not contain (pid)

      val p2 = system.actorOf(TestActor.props(pid))
      p2 ! GetRecoveredState
      expectMsg(RecoveredState("", Nil, 0L))

      queries.currentEventsByTag(tag = "tag-a", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(0)
      queries.currentEventsByTag(tag = "tag-b", offset = NoOffset).runWith(Sink.seq).futureValue.size should ===(0)
    }

    "delete some for one persistenceId" taggedAs (RequiresCassandraThree) in {
      val pid = nextPid
      val p = system.actorOf(TestActor.props(pid))
      (1 to 8).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      system.stop(p)

      val cleanup = new Cleanup(system)
      cleanup.deleteEventsTo(pid, 5).futureValue

      val p2 = system.actorOf(TestActor.props(pid))
      p2 ! GetRecoveredState
      expectMsg(RecoveredState("", List("evt-6", "evt-7", "evt-8"), 8L))
    }

    "clean up before latest snapshot for one persistence id" taggedAs (RequiresCassandraThree) in {
      val pid = nextPid
      val p = system.actorOf(TestActor.props(pid))
      (1 to 3).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      p ! Snap
      expectMsgType[Ack]
      (1 to 3).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      p ! Snap
      expectMsgType[Ack]
      (1 to 3).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      system.stop(p)

      val cleanup = new Cleanup(system)
      cleanup.cleanupBeforeSnapshot(pid, 1).futureValue

      val p2 = system.actorOf(TestActor.props(pid))
      p2 ! GetRecoveredState
      expectMsg(RecoveredState("snap-6", List("evt-7", "evt-8", "evt-9"), 9L))

      // check old snapshots are done
      val snapshots = allSnapshots(pid)
      snapshots.size shouldEqual 1
      snapshots.head.sequenceNr shouldEqual 6
      // check old events are gone
      queries
        .currentEventsByPersistenceId(pid, 0, Long.MaxValue)
        .map(_.event.toString)
        .runWith(Sink.seq)
        .futureValue shouldEqual List("evt-7", "evt-8", "evt-9")
    }

    "clean up before snapshot including timestamp that results in all events kept for one persistence id" taggedAs (RequiresCassandraThree) in {
      val pid = nextPid
      val p = system.actorOf(TestActor.props(pid))
      (1 to 3).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      p ! Snap
      expectMsgType[Ack]
      (1 to 3).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      p ! Snap
      expectMsgType[Ack]
      (1 to 3).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      system.stop(p)

      val cleanup = new Cleanup(system)
      // a long way in the past so keep everything
      cleanup.cleanupBeforeSnapshot(pid, 1, 100).futureValue

      val p2 = system.actorOf(TestActor.props(pid))
      p2 ! GetRecoveredState
      expectMsg(RecoveredState("snap-6", List("evt-7", "evt-8", "evt-9"), 9L))

      // check old snapshots are done
      val snapshots = allSnapshots(pid)
      snapshots.size shouldEqual 2
      // check old events are kept due to timestamp, all events before the oldest snapshot are still deleted
      queries
        .currentEventsByPersistenceId(pid, 0, Long.MaxValue)
        .map(_.event.toString)
        .runWith(Sink.seq)
        .futureValue shouldEqual List("evt-4", "evt-5", "evt-6", "evt-7", "evt-8", "evt-9")
    }

    "clean up before snapshot including timestamp for one persistence id" taggedAs (RequiresCassandraThree) in {
      val pid = nextPid
      val p = system.actorOf(TestActor.props(pid))
      (1 to 3).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      p ! Snap
      expectMsgType[Ack]
      (1 to 3).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      p ! Snap
      expectMsgType[Ack]
      (1 to 3).foreach { i =>
        p ! PersistEvent
        expectMsgType[Ack]
      }
      system.stop(p)

      val cleanup = new Cleanup(system)
      // timestamp shouldn't result in any more events/snapshtos kept apart from the last one
      cleanup.cleanupBeforeSnapshot(pid, 1, System.currentTimeMillis()).futureValue

      val p2 = system.actorOf(TestActor.props(pid))
      p2 ! GetRecoveredState
      expectMsg(RecoveredState("snap-6", List("evt-7", "evt-8", "evt-9"), 9L))

      // check old snapshots are done
      val snapshots = allSnapshots(pid)
      snapshots.size shouldEqual 1
      // check old events are kept due to timestamp
      queries
        .currentEventsByPersistenceId(pid, 0, Long.MaxValue)
        .map(_.event.toString)
        .runWith(Sink.seq)
        .futureValue shouldEqual List("evt-7", "evt-8", "evt-9")
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
      cleanup.deleteAll(List(pidA, pidB, pidC), neverUsePersistenceIdAgain = true).futureValue // FIXME

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

  "Time and snapshot based cleanup" must {
    "keep the correct  number of snapshots" taggedAs (RequiresCassandraThree) in {
      val cleanup = new Cleanup(system)
      val pid = nextPid
      writeTestSnapshot(SnapshotMetadata(pid, 1, 1000), "snapshot-1").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 2, 2000), "snapshot-2").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 3, 3000), "snapshot-3").futureValue

      val oldestSnapshot = cleanup.deleteBeforeSnapshot(pid, 2).futureValue

      val p1 = system.actorOf(TestActor.props(pid))
      p1 ! GetRecoveredState
      expectMsg(RecoveredState("snapshot-3", Nil, 3L))

      allSnapshots(pid) shouldEqual List(SnapshotMetadata(pid, 2, 2000), SnapshotMetadata(pid, 3, 3000))

      oldestSnapshot shouldEqual Some(SnapshotMetadata(pid, 2, 2000))
    }
    "keep the all snapshots if fewer than requested without timestamp" taggedAs (RequiresCassandraThree) in {
      val cleanup = new Cleanup(system)
      val pid = nextPid
      writeTestSnapshot(SnapshotMetadata(pid, 1, 1000), "snapshot-1").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 2, 2000), "snapshot-2").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 3, 3000), "snapshot-3").futureValue

      val oldestSnapshot = cleanup.deleteBeforeSnapshot(pid, 4).futureValue

      val p1 = system.actorOf(TestActor.props(pid))
      p1 ! GetRecoveredState
      expectMsg(RecoveredState("snapshot-3", Nil, 3L))

      allSnapshots(pid) shouldEqual List(
        SnapshotMetadata(pid, 1, 1000),
        SnapshotMetadata(pid, 2, 2000),
        SnapshotMetadata(pid, 3, 3000))

      oldestSnapshot shouldEqual Some(SnapshotMetadata(pid, 1, 1000))
    }
    "keep the all snapshots if fewer than requested with timestamp" taggedAs (RequiresCassandraThree) in {
      val cleanup = new Cleanup(system)
      val pid = nextPid
      writeTestSnapshot(SnapshotMetadata(pid, 1, 1000), "snapshot-1").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 2, 2000), "snapshot-2").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 3, 3000), "snapshot-3").futureValue

      val oldestSnapshot = cleanup.deleteBeforeSnapshot(pid, 4, 1500).futureValue

      val p1 = system.actorOf(TestActor.props(pid))
      p1 ! GetRecoveredState
      expectMsg(RecoveredState("snapshot-3", Nil, 3L))

      allSnapshots(pid) shouldEqual List(
        SnapshotMetadata(pid, 1, 1000),
        SnapshotMetadata(pid, 2, 2000),
        SnapshotMetadata(pid, 3, 3000))

      oldestSnapshot shouldEqual Some(SnapshotMetadata(pid, 1, 1000))
    }

    "work without timestamp when there are no snapshots" in {
      val cleanup = new Cleanup(system)
      val pid = nextPid
      val oldestSnapshot = cleanup.deleteBeforeSnapshot(pid, 2).futureValue
      oldestSnapshot shouldEqual None
    }

    "work with timestamp when there are no snapshots" in {
      val cleanup = new Cleanup(system)
      val pid = nextPid
      val oldestSnapshot = cleanup.deleteBeforeSnapshot(pid, 2, 100).futureValue
      oldestSnapshot shouldEqual None
    }

    "don't delete snapshots newer than the oldest date" taggedAs (RequiresCassandraThree) in {
      val cleanup = new Cleanup(system)
      val pid = nextPid
      writeTestSnapshot(SnapshotMetadata(pid, 1, 1000), "snapshot-1").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 2, 2000), "snapshot-2").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 3, 3000), "snapshot-3").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 4, 4000), "snapshot-4").futureValue

      val oldestSnapshot = cleanup.deleteBeforeSnapshot(pid, 2, 1500).futureValue

      val p1 = system.actorOf(TestActor.props(pid))
      p1 ! GetRecoveredState
      expectMsg(RecoveredState("snapshot-4", Nil, 4L))

      // here 3 snapshots are kept as snapshot 2 is newer than 1500
      allSnapshots(pid) shouldEqual List(
        SnapshotMetadata(pid, 2, 2000),
        SnapshotMetadata(pid, 3, 3000),
        SnapshotMetadata(pid, 4, 4000))

      oldestSnapshot shouldEqual Some(SnapshotMetadata(pid, 2, 2000))
    }
    "keep snapshots older than the oldest date to meet snapshotsToKeep" taggedAs (RequiresCassandraThree) in {
      val cleanup = new Cleanup(system)
      val pid = nextPid
      writeTestSnapshot(SnapshotMetadata(pid, 1, 1000), "snapshot-1").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 2, 2000), "snapshot-2").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 3, 3000), "snapshot-3").futureValue
      writeTestSnapshot(SnapshotMetadata(pid, 4, 4000), "snapshot-4").futureValue

      val oldestSnapshot = cleanup.deleteBeforeSnapshot(pid, 2, 5000).futureValue

      val p1 = system.actorOf(TestActor.props(pid))
      p1 ! GetRecoveredState
      expectMsg(RecoveredState("snapshot-4", Nil, 4L))

      // 2 snapshots are kept that are both older than the keepAfter as 2 should be kept
      allSnapshots(pid) shouldEqual List(SnapshotMetadata(pid, 3, 3000), SnapshotMetadata(pid, 4, 4000))

      oldestSnapshot shouldEqual Some(SnapshotMetadata(pid, 3, 3000))
    }
  }

  private def allSnapshots(pid: String): Seq[SnapshotMetadata] = {
    import scala.collection.JavaConverters._
    cluster
      .execute(s"select * from ${snapshotName}.snapshots where persistence_id = '${pid}' order by sequence_nr")
      .asScala
      .map(row =>
        SnapshotMetadata(row.getString("persistence_id"), row.getLong("sequence_nr"), row.getLong("timestamp")))
      .toList
  }

}
