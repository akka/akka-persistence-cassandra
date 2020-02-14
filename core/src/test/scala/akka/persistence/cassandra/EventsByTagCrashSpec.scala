/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.NotUsed
import akka.persistence.cassandra.TestTaggingActor.{ Ack, Crash }
import akka.persistence.query.{ EventEnvelope, NoOffset }
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink

import scala.concurrent.duration._

class EventsByTagCrashSpec extends CassandraSpec(EventsByTagRestartSpec.config) {

  val waitTime = 100.milliseconds

  "EventsByTag" must {

    "should handle crashes of the persistent actor" in {
      // crash the actor 250 times, persist 5 events each time
      val crashEvery = 5
      val crashNr = 250
      val msgs = crashEvery * crashNr
      val p2 = system.actorOf(TestTaggingActor.props("p2", Set("blue")))
      (1 to msgs).foreach { cn =>
        if (cn % crashEvery == 0) {
          p2 ! Crash
        }
        val msg = s"msg $cn"
        p2 ! msg
        expectMsg(Ack)
      }
      val blueTags: Source[EventEnvelope, NotUsed] = queryJournal.eventsByTag(tag = "blue", offset = NoOffset)
      val tagProbe = blueTags.runWith(TestSink.probe[EventEnvelope](system))
      (1L to msgs).foreach { m =>
        val expected = s"msg $m"
        tagProbe.request(1)
        tagProbe.expectNext().event shouldEqual expected
      }
      tagProbe.expectNoMessage(250.millis)
      tagProbe.cancel()

    }
  }
}
