/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.{ ActorLogging, ActorRef, PoisonPill, Props }
import akka.persistence.{ PersistentActor, RecoveryCompleted }
import akka.persistence.cassandra.CassandraCorruptJournalSpec.{ FullEventLog, GetEventLog, Ping, Pong }
import akka.testkit.{ EventFilter, TestProbe }

object CassandraCorruptJournalSpec {

  case object GetEventLog

  case object Ping

  case object Pong

  class FullEventLog(val persistenceId: String, override val journalPluginId: String)
      extends PersistentActor
      with ActorLogging {

    var events: Vector[String] = Vector.empty

    override def receiveRecover: Receive = {
      case s: String =>
        log.info("Received event during recovery: {}", s)
        events = events :+ s
      case RecoveryCompleted =>
        log.info("Recovery complete: {}", events)

    }

    override def postStop(): Unit = {
      log.info("Stopping with events: {}", events)
    }

    override def receiveCommand: Receive = {
      case s: String =>
        persist(s) { e =>
          events = events :+ s
          sender() ! e + "-done"
        }
      case GetEventLog =>
        sender() ! events
      case Ping =>
        sender() ! Pong
    }
  }

  object FullEventLog {
    def props(persistenceId: String, journalId: String): Props = Props(new FullEventLog(persistenceId, journalId))
  }

}

class CassandraCorruptJournalSpec extends CassandraSpec(s"""
    akka {
      loglevel = debug
      loggers = ["akka.testkit.TestEventListener"]
    }

    cassandra-journal.replay-filter {
   # What the filter should do when detecting invalid events.
   # Supported values:
   # `repair-by-discard-old` : discard events from old writers,
   #                           warning is logged
   # `fail` : fail the replay, error is logged
   # `warn` : log warning but emit events untouched
   # `off` : disable this feature completely
      mode = repair-by-discard-old
      debug = yes
    }

    cassandra-journal-fail = $${cassandra-journal}
    cassandra-journal-fail {
      replay-filter.mode = fail
    }

    cassandra-journal-warn = $${cassandra-journal}
    cassandra-journal-warn {
      replay-filter.mode = warn
    }
  """.stripMargin) {

  def setup(persistenceId: String, journalId: String = "cassandra-journal"): ActorRef = {
    val ref = system.actorOf(FullEventLog.props(persistenceId, journalId))
    ref
  }

  def runConcurrentPersistentActors(journal: String): String = {
    val probe = TestProbe()
    val pid = nextPid
    val p1a = setup(pid, journalId = journal)
    probe.watch(p1a)
    val p1b = setup(pid)
    probe.watch(p1b)

    // Check PAs have finished recovery
    p1a ! Ping
    expectMsg(Pong)
    p1b ! Ping
    expectMsg(Pong)

    p1a ! "p1a-1" // seq 1
    expectMsg("p1a-1-done")

    p1b ! "p1b-1" // seq 1
    expectMsg("p1b-1-done")

    p1a ! "p1a-2" // seq 2
    expectMsg("p1a-2-done")

    p1a ! PoisonPill
    probe.expectTerminated(p1a)
    p1b ! PoisonPill
    probe.expectTerminated(p1b)
    pid
  }

  "Cassandra recovery" must {
    "work with replay-filter = repair-by-discard-old" in {

      val pid = runConcurrentPersistentActors("cassandra-journal")

      EventFilter.warning(pattern = "Invalid replayed event", occurrences = 2).intercept {
        val p1c = setup(pid)
        p1c ! "p1c-1"
        expectMsg("p1c-1-done")
        p1c ! GetEventLog
        expectMsg(Vector("p1b-1", "p1c-1"))
      }
    }

    "work with replay-filter = fail" in {

      val pid = runConcurrentPersistentActors("cassandra-journal-fail")

      EventFilter[IllegalStateException](pattern = "Invalid replayed event", occurrences = 1).intercept {
        setup(pid, journalId = "cassandra-journal-fail")
      }
    }

    "work with replay-filter = warn" in {

      val pid = runConcurrentPersistentActors("cassandra-journal-warn")
      EventFilter.warning(pattern = "Invalid replayed event", occurrences = 2).intercept {
        val p1c = setup(pid, journalId = "cassandra-journal-warn")
        p1c ! "p1c-1"
        expectMsg("p1c-1-done")
        p1c ! GetEventLog
        expectMsg(Vector("p1a-1", "p1b-1", "p1a-2", "p1c-1"))
      }
    }
  }

}
