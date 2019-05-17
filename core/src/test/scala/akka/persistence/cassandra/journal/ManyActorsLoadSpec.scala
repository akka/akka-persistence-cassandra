/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import akka.actor._
import akka.persistence.PersistentActor
import akka.persistence.cassandra.CassandraLifecycle
import akka.persistence.cassandra.CassandraSpec
import akka.persistence.journal.Tagged
import com.typesafe.config.ConfigFactory

object ManyActorsLoadSpec {
  val config = ConfigFactory.parseString(s"""
      akka.loglevel = INFO
      cassandra-journal.keyspace=ManyActorsLoadSpec
      cassandra-journal.events-by-tag.enabled = on
      # increase this to 3s when benchmarking
      cassandra-journal.events-by-tag.scanning-flush-interval = 1s
      #cassandra-journal.log-queries = on
      cassandra-snapshot-store.keyspace=ManyActorsLoadSpecSnapshot
      #cassandra-snapshot-store.log-queries = on
    """).withFallback(CassandraLifecycle.config)

  final case class Init(numberOfEvents: Int)
  case object InitDone
  private final case class Next(remaining: Int)
  final case class Delete(seqNr: Long)
  case object GetMetrics
  final case class Metrics(
      snapshotDuration: FiniteDuration,
      replayDuration1: FiniteDuration,
      replayDuration2: FiniteDuration,
      replayedEvents: Int,
      totalDuration: FiniteDuration)

  def props(persistenceId: String, tagging: Long => Set[String]): Props =
    Props(new ProcessorA(persistenceId, tagging))

  class ProcessorA(val persistenceId: String, tagging: Long => Set[String]) extends PersistentActor {

    def receiveRecover: Receive = {
      case _: String =>
    }

    def receiveCommand: Receive = {
      case s: String =>
        val tags = tagging(lastSequenceNr)
        val event =
          if (tags.isEmpty) s"event-$lastSequenceNr"
          else Tagged(s"event-$lastSequenceNr", tags)
        persist(event) { _ =>
          sender() ! s
        }
    }
  }

}

/**
 * Reproducer for issue #408
 */
class ManyActorsLoadSpec extends CassandraSpec(ManyActorsLoadSpec.config) {

  import ManyActorsLoadSpec._

  "Persisting from many actors" must {

    "not have performance drop when flushing scanning" in {
      val numberOfActors = 1000 // increase this to 10000 when benchmarking

      val rounds = 1 // increase this to 10 when benchmarking
      val deadline =
        Deadline.now + rounds * system.settings.config
          .getDuration("cassandra-journal.events-by-tag.scanning-flush-interval", TimeUnit.MILLISECONDS)
          .millis + 2.seconds

      val tagging: Long => Set[String] = { _ =>
        Set.empty
      }
      //      val tagging: Long => Set[String] = { seqNr =>
      //        if (seqNr % 10 == 0) Set("blue")
      //        else if (seqNr % 17 == 0) Set("blue", "green")
      //        else Set.empty
      //      }

      val actors = (0 until numberOfActors).map { i =>
        system.actorOf(props(persistenceId = s"pid-$i", tagging))
      }.toVector

      actors.foreach(_ ! "init")
      receiveN(numberOfActors, 20.seconds)

      while (deadline.hasTimeLeft()) {
        val startTime = System.nanoTime()

        (0 until numberOfActors).foreach { i =>
          actors(i) ! "x"
        }
        receiveN(numberOfActors, 10.seconds)
        val duration = (System.nanoTime() - startTime).nanos
        println(s"Persisting $numberOfActors events from $numberOfActors actors took: ${duration.toMillis} ms")
      }

    }
  }

}
