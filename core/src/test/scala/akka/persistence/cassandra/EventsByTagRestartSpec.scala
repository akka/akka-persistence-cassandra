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
import org.scalatest.Matchers

object EventsByTagRestartSpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)
  val firstBucketFormat: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyyMMdd'T'HH:mm")

  val config = ConfigFactory.parseString(s"""
       |akka {
       |  actor.debug.unhandled = on
       |}
       |akka.persistence.cassandra {
       |  log-queries = off
       |  events-by-tag {
       |     max-message-batch-size = 250 // make it likely we have messages in the buffer
       |     bucket-size = "Day"
       |  }
       |}
       |
       |akka.actor.serialize-messages=off
    """.stripMargin).withFallback(CassandraLifecycle.config)
}

class EventsByTagRestartSpec extends CassandraSpec(EventsByTagRestartSpec.config) with Matchers {

  implicit val materialiser = ActorMaterializer()(system)

  val waitTime = 100.milliseconds

  "Events by tag recovery for same actor system" must {

    "continue tag sequence nrs for actor restarts" in {
      val messagesPerRestart = 100
      val restarts = 10

      val tag = "blue"
      val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)

      (0 until restarts).foreach { restart =>
        val p1 = system.actorOf(TestTaggingActor.props("p1", Set(tag)))
        val probe = TestProbe()
        probe.watch(p1)
        (1 to messagesPerRestart).foreach { i =>
          p1 ! s"r$restart-e$i"
          expectMsg(Ack)
        }
        p1 ! Stop
      }

      val blueTags = queryJournal.eventsByTag(tag, offset = NoOffset)
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

    "not write incorrect tag sequence nrs when actor is stopped prematurely" in {
      val tag = "green"
      val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val p2 = system.actorOf(TestTaggingActor.props("p2", Set(tag)))
      val probe1 = TestProbe()
      p2.tell("e1", probe1.ref)
      probe1.expectMsg(Ack)
      p2.tell("e2", probe1.ref)
      probe1.expectMsg(Ack)
      p2.tell("e3", probe1.ref)
      probe1.expectMsg(Ack)
      p2.tell("e4", probe1.ref)

      // the PoisonPill will cause the actor to be stopped before the journal write has been completed,
      // and is therefore similar to if the CircuitBreaker would trigger
      val deathProbe = TestProbe()
      deathProbe.watch(p2)
      p2 ! PoisonPill
      deathProbe.expectTerminated(p2)

      val greenTags = queryJournal.eventsByTag(tag, offset = NoOffset)
      val tagProbe = greenTags.runWith(TestSink.probe[Any](system))
      tagProbe.request(10)
      (1 to 3).foreach { n =>
        val event = s"e$n"
        system.log.debug("Expecting event {} sequenceNr {}", event, n)
        tagProbe.expectNextPF { case EventEnvelope(_, "p2", `n`, `event`) => }
      }

      // without the fix this would see a fourth element (with the wrong tagSeqNr)
      // this is racy though, so we could also see that fourth event, but with the right tagSeqNr
      val received = tagProbe.receiveWithin(waitTime)
      received.headOption match {
        case None => // not received
        case Some(evt: EventEnvelope) =>
          evt.event shouldEqual "e4"
        case Some(wat) =>
          fail(s"Unexpected event $wat")
      }
      tagProbe.cancel()
    }

    "use correct tag sequence nrs when actor is stopped prematurely and then restarted" in {
      val tag = "orange"
      val queryJournal = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      val p3 = system.actorOf(TestTaggingActor.props("p3", Set(tag)))
      val probe1 = TestProbe()
      p3.tell("e1", probe1.ref)
      probe1.expectMsg(Ack)
      p3.tell("e2", probe1.ref)
      probe1.expectMsg(Ack)
      p3.tell("e3", probe1.ref)
      probe1.expectMsg(Ack)
      p3.tell("e4", probe1.ref)

      // the PoisonPill will cause the actor to be stopped before the journal write has been completed,
      // and is therefore similar to if the CircuitBreaker would trigger
      val deathProbe = TestProbe()
      deathProbe.watch(p3)
      p3 ! PoisonPill
      deathProbe.expectTerminated(p3)

      // needed to reproduce #562
      Thread.sleep(500)

      val probe2 = TestProbe()
      val p3b = system.actorOf(TestTaggingActor.props("p3", Set(tag)))
      p3b.tell("e5", probe2.ref)
      probe2.expectMsg(Ack)
      p3b.tell("e6", probe2.ref)
      probe2.expectMsg(Ack)

      val greenTags = queryJournal.eventsByTag(tag, offset = NoOffset)
      val tagProbe = greenTags.runWith(TestSink.probe[Any](system))
      tagProbe.request(10)
      // without the fix this would not complete because e4 will have tagSeqNr 1 rather than the expected 4
      (1 to 6).foreach { n =>
        val event = s"e$n"
        system.log.debug("Expecting event {} sequenceNr {}", event, n)
        tagProbe.expectNextPF { case EventEnvelope(_, "p3", `n`, `event`) => }
      }
      tagProbe.expectNoMessage(waitTime)
      tagProbe.cancel()
    }
  }
}
