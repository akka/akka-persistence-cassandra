/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import java.time.format.DateTimeFormatter
import java.time.{ LocalDateTime, ZoneOffset }

import akka.persistence.cassandra.TestTaggingActor.{ Ack, Stop }
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{ EventEnvelope, NoOffset, PersistenceQuery }
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import akka.actor.PoisonPill

object EventsByTagRestartSpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)
  val firstBucketFormat: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyyMMdd'T'HH:mm")

  val config = ConfigFactory.parseString(s"""
       akka {
         loglevel = INFO
       }
       cassandra-journal {
         log-queries = off
         events-by-tag {
            max-message-batch-size = 250 // make it likely we have messages in the buffer
            bucket-size = "Day"
         }
       }

       cassandra-query-journal = {
          first-time-bucket = "${today.minusMinutes(5).format(firstBucketFormat)}"
       }

       akka.actor.serialize-messages=off
    """).withFallback(CassandraLifecycle.config)
}

class EventsByTagRestartSpec extends CassandraSpec(EventsByTagRestartSpec.config) {

  implicit val materialiser = ActorMaterializer()(system)

  val waitTime = 100.milliseconds

  "Events by tag recovery for same actor system" must {

    val messagesPerRestart = 100
    val restarts = 10

    "continue tag sequence nrs for actor restarts" in {
      val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

      (0 until restarts).foreach { restart =>
        val p1 = system.actorOf(TestTaggingActor.props("p1", Set("blue")))
        val probe = TestProbe()
        probe.watch(p1)
        (1 to messagesPerRestart).foreach { i =>
          p1 ! s"r$restart-e$i"
          expectMsg(Ack)
        }
        p1 ! Stop
        probe.expectTerminated(p1)
      }

      val blueTags = queryJournal.eventsByTag(tag = "blue", offset = NoOffset)
      val tagProbe = blueTags.runWith(TestSink.probe[Any](system))
      (0 until restarts).foreach { restart =>
        tagProbe.request(messagesPerRestart + 1)
        (1 to messagesPerRestart).foreach { i =>
          val event = s"r$restart-e$i"
          val sequenceNr = (restart * messagesPerRestart) + i
          system.log.debug("Expecting event {} sequenceNr {}", event, sequenceNr)
          tagProbe.expectNextPF { case EventEnvelope(_, "p1", `sequenceNr`, `event`) => }
        }
      }
      tagProbe.expectNoMessage(waitTime)
      tagProbe.cancel()
    }

    "use correct tag sequence nrs when actor is stopped prematurely" in {
      val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val p2 = system.actorOf(TestTaggingActor.props("p2", Set("green")))
      val probe1 = TestProbe()
      probe1.watch(p2)
      p2.tell("e1", probe1.ref)
      probe1.expectMsg(Ack)
      p2.tell("e2", probe1.ref)
      probe1.expectMsg(Ack)
      p2.tell("e3", probe1.ref)
      probe1.expectMsg(Ack)
      p2.tell("e4", probe1.ref)

      // the PoisonPill will cause the actor to be stopped before the journal write has been completed,
      // and is therefore similar to if the CircuitBreaker would trigger
      p2 ! PoisonPill
      probe1.expectTerminated(p2)

      // needed to reproduce #562
      Thread.sleep(500)

      val probe2 = TestProbe()
      // tag will be missing until we restart actor, (is that ok?)
      val p2b = system.actorOf(TestTaggingActor.props("p2", Set("green")))
      p2b.tell("e5", probe2.ref)
      probe2.expectMsg(Ack)
      p2b.tell("e6", probe2.ref)
      probe2.expectMsg(Ack)

      val greenTags = queryJournal.eventsByTag(tag = "green", offset = NoOffset)
      val tagProbe = greenTags.runWith(TestSink.probe[Any](system))
      tagProbe.request(10)
      (1 to 6).foreach { n =>
        val event = s"e$n"
        system.log.debug("Expecting event {} sequenceNr {}", event, n)
        tagProbe.expectNextPF { case EventEnvelope(_, "p2", `n`, `event`) => }
      }
      tagProbe.expectNoMessage(waitTime)
      tagProbe.cancel()
    }
  }
}
