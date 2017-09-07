/*
 * Copyright (C) 2016 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.persistence.cassandra.query

import java.time.{ LocalDateTime, ZoneOffset }
import java.time.temporal.ChronoUnit
import java.util.UUID

import akka.persistence.PersistentRepr
import akka.persistence.query.{ EventEnvelope, NoOffset, TimeBasedUUID }
import akka.persistence.query.scaladsl.{
  CurrentEventsByTagQuery,
  EventsByTagQuery
}
import scala.concurrent.duration._
import akka.stream.testkit.scaladsl.TestSink

class EventsByTagWithTagPrefixEnablesSpec
  extends AbstractEventsByTagSpec(
    "EventsByTagWithTagPrefixEnablesSpec",
    EventsByTagSpec.tagPrefixConfig
  ) {

  import EventsByTagSpec._

  "Cassandra query currentEventsByTag" must {
    "implement standard CurrentEventsByTagQuery" in {
      queries.isInstanceOf[CurrentEventsByTagQuery] should ===(true)
    }

    "find existing events" in {
      val a = system.actorOf(TestActor.props("a"))
      val b = system.actorOf(TestActor.props("b"))
      a ! "green apple"
      expectMsg(5.seconds, s"green apple-done")
      a ! "black car"
      expectMsg(s"black car-done")
      b ! "yellow apple"
      expectMsg(s"yellow apple-done")
      a ! "red apple"
      expectMsg(s"red apple-done")
      b ! "green leaf"
      expectMsg(s"green leaf-done")
      b ! "yellow car"
      expectMsg(s"yellow car-done")
      a ! "green day"
      expectMsg(s"green day-done")

      val allSrc =
        queries.currentEventsByTag(tag = "all", offset = NoOffset)
      val probe = allSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF {
        case e @ EventEnvelope(_, "a", 1L, "green apple") => e
      }
      probe.expectNextPF {
        case e @ EventEnvelope(_, "a", 2L, "black car") => e
      }
      probe.expectNoMsg(500.millis)
      probe.request(5)
      probe.expectNextPF {
        case e @ EventEnvelope(_, "b", 1L, "yellow apple") => e
      }
      probe.expectNextPF {
        case e @ EventEnvelope(_, "a", 3L, "red apple") => e
      }
      probe.expectNextPF {
        case e @ EventEnvelope(_, "b", 2L, "green leaf") => e
      }
      probe.expectNextPF {
        case e @ EventEnvelope(_, "b", 3L, "yellow car") => e
      }
      probe.expectNextPF {
        case e @ EventEnvelope(_, "a", 4L, "green day") => e
      }
      probe.expectComplete()

      val greenSrc =
        queries.currentEventsByTag(tag = "color:green", offset = NoOffset)
      val probe2 = greenSrc.runWith(TestSink.probe[Any])
      probe2.request(4)
      probe2.expectNextPF {
        case e @ EventEnvelope(_, "a", 1L, "green apple") => e
      }
      probe2.expectNextPF {
        case e @ EventEnvelope(_, "b", 2L, "green leaf") => e
      }
      probe2.expectNextPF {
        case e @ EventEnvelope(_, "a", 4L, "green day") => e
      }
      probe2.expectComplete()

      val carSrc =
        queries.currentEventsByTag(tag = "kind:car", offset = NoOffset)
      val probe3 = carSrc.runWith(TestSink.probe[Any])
      probe3.request(5)
      probe3.expectNextPF {
        case e @ EventEnvelope(_, "a", 2L, "black car") => e
      }
      probe3.expectNextPF {
        case e @ EventEnvelope(_, "b", 3L, "yellow car") => e
      }
      probe3.expectComplete()
    }

    "complete when no events" in {
      val src =
        queries.currentEventsByTag(tag = "color:pink", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectComplete()
    }

    "not see new events after demand request" in {
      val c = system.actorOf(TestActor.props("c"))

      val greenSrc =
        queries.currentEventsByTag(tag = "color:green", offset = NoOffset)
      val probe = greenSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF {
        case e @ EventEnvelope(_, "a", 1L, "green apple") => e
      }
      probe.expectNextPF {
        case e @ EventEnvelope(_, "b", 2L, "green leaf") => e
      }
      probe.expectNoMsg(100.millis)

      c ! "green cucumber"
      expectMsg(s"green cucumber-done")

      probe.expectNoMsg(100.millis)
      probe.request(5)
      probe.expectNextPF {
        case e @ EventEnvelope(_, "a", 4L, "green day") => e
      }
      probe.expectComplete() // green cucumber not seen
    }

    "find events from timestamp offset" in {
      val greenSrc1 =
        queries.currentEventsByTag(tag = "color:green", offset = NoOffset)
      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
      probe1.request(2)
      val appleOffs = probe1
        .expectNextPF {
          case e @ EventEnvelope(_, "a", 1L, "green apple") => e
        }
        .offset
        .asInstanceOf[TimeBasedUUID]
      val leafOffs = probe1
        .expectNextPF {
          case e @ EventEnvelope(_, "b", 2L, "green leaf") => e
        }
        .offset
        .asInstanceOf[TimeBasedUUID]
      probe1.cancel()

      val appleTimestamp = queries.timestampFrom(appleOffs)
      val leafTimestamp = queries.timestampFrom(leafOffs)
      leafTimestamp should be <= (leafTimestamp)

      val greenSrc2 =
        queries.currentEventsByTag(
          tag = "color:green",
          queries.timeBasedUUIDFrom(leafTimestamp)
        )
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      if (appleTimestamp == leafTimestamp)
        probe2.expectNextPF {
          case e @ EventEnvelope(_, "a", 1L, "green apple") => e
        }
      probe2.expectNextPF {
        case e @ EventEnvelope(_, "b", 2L, "green leaf") => e
      }
      probe2.expectNextPF {
        case e @ EventEnvelope(_, "a", 4L, "green day") => e
      }
      probe2.cancel()
    }

    "find events from UUID offset" in {
      val greenSrc1 =
        queries.currentEventsByTag(tag = "color:green", offset = NoOffset)
      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
      probe1.request(2)
      probe1.expectNextPF {
        case e @ EventEnvelope(_, "a", 1L, "green apple") => e
      }
      val offs = probe1.expectNextPF {
        case e @ EventEnvelope(_, "b", 2L, "green leaf") => e
      }.offset
      probe1.cancel()

      val greenSrc2 = queries.currentEventsByTag(tag = "color:green", offs)
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      probe2.expectNextPF {
        case e @ EventEnvelope(_, "a", 4L, "green day") => e
      }
      probe2.cancel()
    }
  }
  "Cassandra live eventsByTag" must {
    "implement standard EventsByTagQuery" in {
      queries.isInstanceOf[EventsByTagQuery] should ===(true)
    }

    "find new events" in {
      val d = system.actorOf(TestActor.props("d"))

      val blackSrc = queries.eventsByTag(tag = "color:black", offset = NoOffset)
      val probe = blackSrc.runWith(TestSink.probe[Any])
      probe.request(2)
      probe.expectNextPF {
        case e @ EventEnvelope(_, "a", 2L, "black car") => e
      }
      probe.expectNoMsg(100.millis)

      d ! "black dog"
      expectMsg(s"black dog-done")
      d ! "black night"
      expectMsg(s"black night-done")

      probe.expectNextPF {
        case e @ EventEnvelope(_, "d", 1L, "black dog") => e
      }
      probe.expectNoMsg(100.millis)
      probe.request(10)
      probe.expectNextPF {
        case e @ EventEnvelope(_, "d", 2L, "black night") => e
      }
      probe.cancel()
    }

    "find events from timestamp offset" in {
      val greenSrc1 =
        queries.eventsByTag(tag = "color:green", offset = NoOffset)
      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
      probe1.request(2)
      val appleOffs = probe1
        .expectNextPF {
          case e @ EventEnvelope(_, "a", 1L, "green apple") => e
        }
        .offset
        .asInstanceOf[TimeBasedUUID]
      val leafOffs = probe1
        .expectNextPF {
          case e @ EventEnvelope(_, "b", 2L, "green leaf") => e
        }
        .offset
        .asInstanceOf[TimeBasedUUID]
      probe1.cancel()

      val appleTimestamp = queries.timestampFrom(appleOffs)
      val leafTimestamp = queries.timestampFrom(leafOffs)
      appleTimestamp should be <= (leafTimestamp)

      val greenSrc2 =
        queries.currentEventsByTag(
          tag = "color:green",
          queries.timeBasedUUIDFrom(leafTimestamp)
        )
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      if (appleTimestamp == leafTimestamp)
        probe2.expectNextPF {
          case e @ EventEnvelope(_, "a", 1L, "green apple") => e
        }
      probe2.expectNextPF {
        case e @ EventEnvelope(_, "b", 2L, "green leaf") => e
      }
      probe2.expectNextPF {
        case e @ EventEnvelope(_, "a", 4L, "green day") => e
      }
      probe2.expectNextPF {
        case e @ EventEnvelope(_, "c", 1L, "green cucumber") => e
      }
      probe2.expectNoMsg(100.millis)
      probe2.cancel()
    }

    "find events from UUID offset " in {
      val greenSrc1 =
        queries.eventsByTag(tag = "color:green", offset = NoOffset)
      val probe1 = greenSrc1.runWith(TestSink.probe[Any])
      probe1.request(2)
      probe1.expectNextPF {
        case e @ EventEnvelope(_, "a", 1L, "green apple") => e
      }
      val offs = probe1.expectNextPF {
        case e @ EventEnvelope(_, "b", 2L, "green leaf") => e
      }.offset
      probe1.cancel()

      val greenSrc2 = queries.eventsByTag(tag = "color:green", offs)
      val probe2 = greenSrc2.runWith(TestSink.probe[Any])
      probe2.request(10)
      probe2.expectNextPF {
        case e @ EventEnvelope(_, "a", 4L, "green day") => e
      }
      probe2.expectNextPF {
        case e @ EventEnvelope(_, "c", 1L, "green cucumber") => e
      }
      probe2.expectNoMsg(100.millis)
      probe2.cancel()
    }

    "stream many events" in {
      val e = system.actorOf(TestActor.props("e"))

      val src = queries.eventsByTag(tag = "color:purple", offset = NoOffset)
      val probe = src.runWith(TestSink.probe[Any])

      for (n <- 1 to 100)
        e ! s"purple $n"

      probe.request(200)
      for (n <- 1 to 100) {
        val Expected = s"purple $n"
        probe.expectNextPF { case e @ EventEnvelope(_, "e", _, Expected) => e }
      }
      probe.expectNoMsg(100.millis)

      for (n <- 101 to 200)
        e ! s"purple $n"

      for (n <- 101 to 200) {
        val Expected = s"purple $n"
        probe.expectNextPF { case e @ EventEnvelope(_, "e", _, Expected) => e }
      }
      probe.expectNoMsg(100.millis)

      probe.request(10)
      probe.expectNoMsg(100.millis)
    }
  }

  override def remainingOrDefault = 2.seconds
}
