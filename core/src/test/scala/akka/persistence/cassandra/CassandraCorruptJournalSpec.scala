/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.actor.{ ActorRef, PoisonPill }
import akka.persistence.cassandra.query.{ DirectWriting, TestActor }
import akka.testkit.EventFilter

class CassandraCorruptJournalSpec extends CassandraSpec(
  """
    akka {
      loglevel = DEBUG
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
    }

    cassandra-journal-fail = ${cassandra-journal}
    cassandra-journal-fail {
      replay-filter.mode = fail
    }

    cassandra-journal-warn = ${cassandra-journal}
    cassandra-journal-warn {
      replay-filter.mode = warn
    }
  """.stripMargin) with DirectWriting {

  def setup(persistenceId: String, journalId: String = "cassandra-journal"): ActorRef = {
    val ref = system.actorOf(TestActor.props(persistenceId, journalId))
    ref
  }

  "Cassandra recovery" must {
    "work with replay-filter = repair-by-discard-old" in {
      val pid = nextPid
      val p1a = setup(pid)
      val p1b = setup(pid)

      p1a ! "p1a-1"
      expectMsg("p1a-1-done")
      p1a ! "p1a-2"
      expectMsg("p1a-2-done")
      p1b ! "p1b-1"
      expectMsg("p1b-1-done")

      p1a ! PoisonPill
      p1b ! PoisonPill

      // Two logs. One for the new writer sequenceNr 1, another for the old writer sequenceNr 2
      EventFilter.warning(pattern = "Invalid replayed event", occurrences = 2) intercept {
        val p1c = setup(pid)
        p1c ! "p1c-1"
        expectMsg("p1c-1-done")
      }
    }

    "work with replay-filter = fail" in {
      val pid = nextPid
      val p1a = setup(pid, journalId = "cassandra-journal-fail")
      val p1b = setup(pid, journalId = "cassandra-journal-fail")

      p1a ! "p1a-1"
      expectMsg("p1a-1-done")
      p1a ! "p1a-2"
      expectMsg("p1a-2-done")
      p1b ! "p1b-1"
      expectMsg("p1b-1-done")

      p1a ! PoisonPill
      p1b ! PoisonPill

      EventFilter[IllegalStateException](pattern = "Invalid replayed event", occurrences = 1) intercept {
        setup(pid, journalId = "cassandra-journal-fail")
      }
    }

    "work with replay-filter = warn" in {
      val pid = nextPid
      val p1a = setup(pid, journalId = "cassandra-journal-warn")
      val p1b = setup(pid, journalId = "cassandra-journal-warn")

      p1a ! "p1a-1"
      expectMsg("p1a-1-done")
      p1a ! "p1a-2"
      expectMsg("p1a-2-done")
      p1b ! "p1b-1"
      expectMsg("p1b-1-done")

      p1a ! PoisonPill
      p1b ! PoisonPill

      EventFilter.warning(pattern = "Invalid replayed event", occurrences = 2) intercept {
        val p1c = setup(pid)
        p1c ! "p1c-1"
        expectMsg("p1c-1-done")
      }
    }
  }

}
