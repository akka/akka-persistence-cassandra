/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
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
import scala.util.Try

object EventsByTagRestartSpec {
  val today = LocalDateTime.now(ZoneOffset.UTC)
  val firstBucketFormat: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyyMMdd'T'HH:mm")

  val config = ConfigFactory.parseString(
    s"""
       |akka {
       |  loglevel = INFO
       |  actor.debug.unhandled = on
       |}
       |cassandra-journal {
       |  log-queries = off
       |  events-by-tag {
       |     max-message-batch-size = 250 // make it likely we have messages in the buffer
       |     bucket-size = "Day"
       |  }
       |}
       |
       |cassandra-query-journal = {
       |   first-time-bucket = "${today.minusMinutes(5).format(firstBucketFormat)}"
       |}
       |
       |akka.actor.serialize-messages=off
    """.stripMargin).withFallback(CassandraLifecycle.config)
}

class EventsByTagRestartSpec extends CassandraSpec(EventsByTagRestartSpec.config) {

  lazy val session = {
    cluster.connect(journalName)
  }

  override protected def afterAll(): Unit = {
    Try {
      session.close()
      cluster.close()
    }
    super.afterAll()
  }

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
        (1 to messagesPerRestart) foreach { i =>
          p1 ! s"r$restart-e$i"
          expectMsg(Ack)
        }
        p1 ! Stop
        probe.expectTerminated(p1)
      }

      val greenTags = queryJournal.eventsByTag(tag = "blue", offset = NoOffset)
      val tagProbe = greenTags.runWith(TestSink.probe[Any](system))
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
  }
}
